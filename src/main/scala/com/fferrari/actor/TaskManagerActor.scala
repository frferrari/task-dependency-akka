package com.fferrari.actor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.fferrari.actor.protocol.TaskManagerResponseProtocol.WrappedTaskResponse
import com.fferrari.actor.protocol.{TaskManagerRequestProtocol, TaskManagerResponseProtocol, TaskRequestProtocol, TaskResponseProtocol}
import com.fferrari.model.ServiceDeployment
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object TaskManagerActor {

  val checkHealthTimeout = 3.seconds

  case class InvalidServiceSpecificationException(message: String) extends Exception(message)

  case class EmptyServiceSpecificationException(message: String) extends Exception(message)

  /*
   * A Task represents a Microservice and it is handled through Akka actors
   * We need specific equals/hashCode methods for the Graph that represents the deployment
   * service to be properly handled
   */
  case class Task(id: Int, name: String, isEntryPoint: Boolean, replicas: Int) {
    override def equals(other: Any): Boolean = other match {
      case that: Task => that.name == this.name
      case that: String => that == this.name
      case _ => false
    }

    override def hashCode(): Int = name.##
  }

  def apply(): Behavior[TaskManagerRequestProtocol.Request] = Behaviors.setup[TaskManagerRequestProtocol.Request] { context =>
    manageRequests(context.messageAdapter(WrappedTaskResponse))
  }

  /**
   * The main Behavior that handles the following messages:
   *   TaskManagerRequestProtocol.Deploy
   *   TaskManagerRequestProtocol.CheckHealth
   * @param taskResponseMapper An adapter for the Actor message
   * @param g The Graph containing the Nodes for the services to deploy
   * @param routers A Map containing the ActorRef of each Task router
   * @return The next Behavior to switch to
   */
  def manageRequests(taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                     g: Graph[Task, DiEdge] = Graph.empty[Task, DiEdge],
                     routers: Map[String, ActorRef[TaskRequestProtocol.Request]] = Map()): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.receive { (context, message) =>
      message match {
        case TaskManagerRequestProtocol.Deploy(services, replyTo) =>

          // Stop any existing deployment
          // val newRouters = cleanUp(routers)(context)
          // Thread.sleep(3000) // TODO: Nobody wants to Thread.sleep, we should check for the Actors to be stopped
          val newRouters = routers

          // Build the Service Deployment Graph
          buildServiceDeploymentGraph(services) match {
            case newGraph if newGraph.isCyclic =>
              // Reject the deployment request if the Graph is cyclic
              context.log.error("The service deployment is cyclic, stopping")
              replyTo ! TaskManagerResponseProtocol.DeploymentError
              manageRequests(taskResponseMapper, newGraph, newRouters)

            case newGraph =>
              // If the Graph is acyclic then we can spawn the Tasks
              spawnTasks(newGraph, taskResponseMapper, replyTo)(context)
          }

        case TaskManagerRequestProtocol.CheckHealth(replyTo) =>
          getTopology(g) match {
            case Success(topology) =>
              // Send a CheckHealth message to ALL routers
              broadcastHealthCheck(topology.map(_.value), routers, taskResponseMapper)

              // Check that ALL Routers reply with a TaskIsHealthy message
              tasksHealthChecking(taskResponseMapper, g, routers, topology.size, replyTo)

            case Failure(e) =>
              context.log.error(e.getMessage)
              Behaviors.same
          }
      }
    }

  /**
   * Allows to broadcast the CheckHealth message to the provided list of Tasks
   * @param tasks The list of Tasks to broadcast the CheckHealth message to
   * @param routers The list of Routers to broadcast the CheckHealth message to
   * @param taskResponseMapper An adapter for the Actor message
   */
  def broadcastHealthCheck(tasks: List[Task],
                           routers: Map[String, ActorRef[TaskRequestProtocol.Request]],
                           taskResponseMapper: ActorRef[TaskResponseProtocol.Response]): Unit = {
    for {
      task <- tasks
      router <- routers.get(task.name)
    } yield {
      router ! TaskRequestProtocol.CheckHealth(taskResponseMapper)
    }

    ()
  }

  /**
   * Handles the reply of each Task of the service deployment to the HealthCheck message sent to them.
   * It awaits maximum `checkHealthTimeout` for each reply per Task :
   *   in case of Timeout it sends back an HealthStatus(false) to the replyTo actor
   *   in case all the replies have been received without Timeout, it sends back an HealthStatus(true) to the replyTo actor
   * @param taskResponseMapper An adapter for the Actor message
   * @param g The Graph containing the Nodes for the services to deploy
   * @param routers A Map containing the ActorRef of each Task router
   * @param taskCount A counf of Task for which to await a reply
   * @param replyTo The ActorRef for the reply status
   * @return The next Behavior to switch to
   */
  def tasksHealthChecking(taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                          g: Graph[Task, DiEdge],
                          routers: Map[String, ActorRef[TaskRequestProtocol.Request]],
                          taskCount: Int,
                          replyTo: ActorRef[TaskManagerResponseProtocol.Response]): Behavior[TaskManagerRequestProtocol.Request] = {
    Behaviors.withTimers { timer =>
      // We allow some time for each Task to reply to the HealthCheck message
      timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, checkHealthTimeout)

      Behaviors.receive[TaskManagerRequestProtocol.Request] { (context, message) =>
        message match {
          case wrapped: TaskManagerResponseProtocol.WrappedTaskResponse =>
            wrapped.response match {
              case TaskResponseProtocol.TaskIsHealthy =>
                context.log.info(s"Received TaskIsHealthy, left with $taskCount Task(s) to check")

                val newTaskCount = taskCount - 1

                if (newTaskCount <= 0) {
                  timer.cancelAll()
                  context.log.info("All Tasks are healthy")
                  replyTo ! TaskManagerResponseProtocol.HealthStatus(isHealthy = true)
                  manageRequests(taskResponseMapper, g, routers)
                } else {
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, checkHealthTimeout)
                  tasksHealthChecking(taskResponseMapper, g, routers, newTaskCount, replyTo)
                }
            }

          case TaskManagerRequestProtocol.HealthCheckTimeout =>
            context.log.error("A HealthCheck Timeout has been received, as least one service is not healthy")
            replyTo ! TaskManagerResponseProtocol.HealthStatus(isHealthy = false)
            manageRequests(taskResponseMapper, g, routers)
        }
      }
    }
  }

  /**
   * Creates a graph for the provided service deployment
   * @param services A list of services to deploy
   * @return The Graph of tasks for the provided service deployment
   */
  def buildServiceDeploymentGraph(services: List[ServiceDeployment]): Graph[Task, DiEdge] =
    createEdges(services, createNodes(services))

  /**
   * Creates the Nodes of a Graph corresponding to a given list of services
   * @param services A list of services to deploy
   * @return The Graph of Noddes
   */
  def createNodes(services: List[ServiceDeployment]): Graph[Task, DiEdge] = {
    val g = Graph.empty[Task, DiEdge]

    for {
      (service, idx) <- services.zipWithIndex
    } yield g += Task(id = idx, name = service.serviceName, isEntryPoint = service.entryPoint, replicas = service.replicas)

    g
  }

  /**
   * Creates the Edges of a Graph corresponding to a given list of services
   * @param services A list of services to deploy
   * @param g The Graph containing the Nodes for the services to deploy
   */
  def createEdges(services: List[ServiceDeployment], g: Graph[Task, DiEdge]): Graph[Task, DiEdge] = {
    def nodeSelection(lookupNodeName: String)(node: g.NodeT): Boolean = node == lookupNodeName

    for {
      service <- services
      dependency <- service.dependencies
      parent <- (g.nodes find (g having (node = nodeSelection(service.serviceName)))).map(_.value)
      children <- (g.nodes find (g having (node = nodeSelection(dependency)))).map(_.value)
    } yield {
      g += parent ~> children
    }

    g
  }

  /**
   * Given a Graph instantiated from a list of services, it spawns the Tasks (as Akka actors) in the proper order
   * based on the Tasks dependencies. Note that it uses Akka routing mechanism to spawn replicas too
   * @param g A Graph containing Nodes/Edges for the services to deploy
   * @param context An actor context
   * @return When successful it returns the provided Graph and a Map of Tasks and their ActorRef
   */
  def spawnTasks(g: Graph[Task, DiEdge],
                 taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                 replyTo: ActorRef[TaskManagerResponseProtocol.Response])
                (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Behavior[TaskManagerRequestProtocol.Request] = {
    g.topologicalSort match {
      case Right(topology) =>
        context.self ! TaskManagerRequestProtocol.SpawnNextTask
        spawnWithCheck(topology.toList.reverse.map(_.value), g, taskResponseMapper, replyTo)

      case Left(_) =>
        replyTo ! TaskManagerResponseProtocol.DeploymentError
        manageRequests(taskResponseMapper, g)
    }
  }

  /**
   * Spawns all the tasks from the provided list of Task, checking that each new Task is healthy before spawning the next Task
   * @param tasks The list of Tasks to spawn
   * @param g A Graph containing Nodes/Edges for the services to deploy
   * @param taskResponseMapper An adapter for the Actor message
   * @param replyTo The ActorRef for the reply status
   * @param routers A Map containing the ActorRef of each Task router
   * @return The next Behavior to switch to
   */
  def spawnWithCheck(tasks: List[Task],
                     g: Graph[Task, DiEdge],
                     taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                     replyTo: ActorRef[TaskManagerResponseProtocol.Response],
                     routers: Map[String, ActorRef[TaskRequestProtocol.Request]] = Map()): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.withTimers { timer =>

      def spawnNextTask(tasks: List[Task],
                        routers: Map[String, ActorRef[TaskRequestProtocol.Request]]): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
        Behaviors.receive[TaskManagerRequestProtocol.Request] { (context, message) =>
          message match {
            case TaskManagerRequestProtocol.SpawnNextTask =>
              tasks match {
                case task :: t =>
                  val router = spawnTask(task)(context)
                  router ! TaskRequestProtocol.CheckHealth(taskResponseMapper.ref)
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, checkHealthTimeout)

                  awaitHealthCheckStatus(tasks, task, router, routers)

                case _ =>
                  context.log.info("All Tasks were successfully deployed")
                  timer.cancelAll()
                  replyTo ! TaskManagerResponseProtocol.DeploymentSuccessful
                  manageRequests(taskResponseMapper, g, routers)
              }
          }
        }

      def awaitHealthCheckStatus(tasks: List[Task],
                                 task: Task,
                                 router: ActorRef[TaskRequestProtocol.Request],
                                 routers: Map[String, ActorRef[TaskRequestProtocol.Request]]): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
        Behaviors.receive[TaskManagerRequestProtocol.Request] { (context, message) =>
          message match {
            case wrapped: TaskManagerResponseProtocol.WrappedTaskResponse =>
              wrapped.response match {
                case TaskResponseProtocol.TaskIsHealthy =>
                  context.log.info("Received TaskIsHealthy message, spawning next Task")
                  context.self ! TaskManagerRequestProtocol.SpawnNextTask
                  spawnNextTask(tasks.tail, routers + (task.name -> router))

                case msg =>
                  context.log.error(s"Unexpected message received while waiting for a Task health check status ($msg)")
                  cleanUp(routers)(context)
                  replyTo ! TaskManagerResponseProtocol.DeploymentError
                  manageRequests(taskResponseMapper, Graph(), Map())
              }

            case TaskManagerRequestProtocol.HealthCheckTimeout =>
              context.log.error("A HealthCheck Timeout has been received while waiting for a spawned Task")
              cleanUp(routers)(context)
              replyTo ! TaskManagerResponseProtocol.DeploymentError
              manageRequests(taskResponseMapper, Graph(), Map())
          }
        }

      spawnNextTask(tasks, routers)
    }

  /**
   * Spawns one Task (and its replicas)
   * @param task The Task to be spawned
   * @param context An actor context
   * @return The ActorRef of the spawned Task
   */
  def spawnTask(task: Task)(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): ActorRef[TaskRequestProtocol.Request] = {
    val replicas = if (task.replicas <= 0) 1 else task.replicas
    context.log.info(s"Spawning task ${task.name} with ${replicas} replicas")
    val pool = Routers.pool(poolSize = replicas)(Behaviors.supervise(TaskActor()).onFailure[Exception](SupervisorStrategy.restart))

    context.spawn(pool, task.name)
  }

  /**
   * Stops all the actors from a provided list of actors
   * @param routers A Map containing the ActorRef of each Task router
   * @param context An actor context
   */
  def cleanUp(routers: Map[String, ActorRef[TaskRequestProtocol.Request]])
             (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Map[String, ActorRef[TaskRequestProtocol.Request]] = {
    context.log.info("Stopping all Tasks ...")

    routers
      .foreach {
        case (taskName, route) =>
          context.log.info(s"Stopping Task $route")
          route ! TaskRequestProtocol.Stop
      }

    Map.empty[String, ActorRef[TaskRequestProtocol.Request]]
  }

  /**
   * Provides the topology of the Graph, this allows us to spawn the Tasks in the proper order based on their dependencies
   * @param g A Graph containing Nodes/Edges for the services to deploy
   * @return A list of Nodes (Tasks)
   */
  def getTopology(g: Graph[Task, DiEdge]): Try[List[g.NodeT]] = {
    g.topologicalSort match {
      case Right(topology) =>
        topology.toList match {
          case topo@h :: t =>
            Success(topo)
          case _ =>
            Failure(EmptyServiceSpecificationException("The graph topology is empty"))
        }
      case Left(_) =>
        Failure(InvalidServiceSpecificationException("Could not generate the graph topology due to an invalid service specification"))
    }
  }
}
