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

  type TaskGraph = Graph[Task, DiEdge]
  type TaskRouter = ActorRef[TaskRequestProtocol.Request]
  type TaskRouters = Map[String, TaskRouter]

  val checkHealthTimeout = 3.seconds

  case class InvalidServiceSpecificationException(message: String) extends Exception(message)

  case class EmptyServiceSpecificationException(message: String) extends Exception(message)

  /*
   * A Task represents a Microservice and it is handled through Akka actors
   * We need specific equals/hashCode methods for the Graph that represents the deployment
   * service to be properly handled
   */
  case class Task(id: Int, name: String, isEntryPoint: Boolean, replicas: Int, dependencies: List[String]) {
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
                     g: TaskGraph = Graph(),
                     routers: TaskRouters = Map()): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.receive { (context, message) =>
      message match {
        case TaskManagerRequestProtocol.Deploy(services, replyTo) =>

          // Build the Service Deployment Graph
          buildServiceDeploymentGraph(services) match {
            case newGraph if newGraph.isCyclic =>
              // Reject the deployment request if the Graph is cyclic
              context.log.error("The service deployment is cyclic, stopping")
              replyTo ! TaskManagerResponseProtocol.DeploymentError
              manageRequests(taskResponseMapper, newGraph, routers)

            case newGraph =>
              // If the Graph is acyclic then we can spawn the Tasks
              spawnTasks(newGraph, taskResponseMapper, replyTo)(context)
          }

        case TaskManagerRequestProtocol.CheckHealth(replyTo) =>
          getTopology(g) match {
            case Success(_) =>
              tasksHealthCheck(g, routers, taskResponseMapper, replyTo)(context)

            case Failure(e) =>
              context.log.error(e.getMessage)
              replyTo ! TaskManagerResponseProtocol.ServiceIsNotDeployed
              Behaviors.same
          }
      }
    }

  /**
   * Allows to check that all the Tasks and their dependencies are healthy,
   * It starts from the entryPoint task and goes down the Graph of Tasks, and for each dependent Task,
   *   it sends a CheckHealth message and waits for the reply.
   * In case a Task does not answer in the appropriate time range, the service is considered unhealthy.
   * @param g The Graph containing the Nodes for the services to deploy
   * @param routers The list of Routers to broadcast the CheckHealth message to
   * @param taskResponseMapper An adapter for the Actor message
   * @param replyTo The ActorRef for the reply status
   * @param context An actor context
   * @return The next Behavior to switch to
   */
  def tasksHealthCheck(g: TaskGraph,
                       routers: TaskRouters,
                       taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                       replyTo: ActorRef[TaskManagerResponseProtocol.Response])
                      (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.withTimers { timer =>
      def taskHealthCheck(itTasks: Iterator[Task]): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
        Behaviors.receive[TaskManagerRequestProtocol.Request] { (context, message) =>
          message match {
            case TaskManagerRequestProtocol.HealthCheckNextTask =>
              itTasks.nextOption match {
                case Some(task) if task.dependencies.nonEmpty =>
                  val expectedReplies: Int = healthCheckDependencies(task)
                  context.log.info(s"Task ${task.name} has ${expectedReplies} dependencies to be health checked")
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, checkHealthTimeout)
                  awaitHealthCheckReplies(itTasks, expectedReplies)

                case Some(task) if task.dependencies.isEmpty =>
                  context.log.info(s"Task ${task.name} has no dependencies, moving to the next Task")
                  context.self ! TaskManagerRequestProtocol.HealthCheckNextTask
                  taskHealthCheck(itTasks)

                case None =>
                  context.log.info("All Tasks and their dependencies have been HealthChecked successfully")
                  replyTo ! TaskManagerResponseProtocol.HealthStatus(isHealthy = true)
                  manageRequests(taskResponseMapper, g, routers)
              }
          }
        }

      def awaitHealthCheckReplies(itTasks: Iterator[Task], expectedReplies: Int): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
        Behaviors.receive[TaskManagerRequestProtocol.Request] { (context, message) =>
          message match {
            case wrapped: TaskManagerResponseProtocol.WrappedTaskResponse =>
              wrapped.response match {
                case TaskResponseProtocol.TaskIsHealthy if expectedReplies <= 1 =>
                  timer.cancelAll()
                  context.log.info(s"Received TaskIsHealthy from a dependent Task, no more dependent Task to check")
                  context.self ! TaskManagerRequestProtocol.HealthCheckNextTask
                  taskHealthCheck(itTasks)

                case TaskResponseProtocol.TaskIsHealthy =>
                  val newExpectedReplies = expectedReplies - 1
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, checkHealthTimeout)
                  context.log.info(s"Received TaskIsHealthy from a dependent Task, left with $newExpectedReplies dependent Task(s) to check")
                  awaitHealthCheckReplies(itTasks, newExpectedReplies)
              }

            case TaskManagerRequestProtocol.HealthCheckTimeout =>
              context.log.error("A HealthCheck Timeout has been received, at least one service is not healthy")
              replyTo ! TaskManagerResponseProtocol.HealthStatus(isHealthy = false)
              manageRequests(taskResponseMapper, g, routers)
          }
        }

      // Sends a CheckHealth request to each `dependent Task` of the provided Task, and returns the number of expected replies
      def healthCheckDependencies(task: Task): Int = {
        task
          .dependencies
          .flatMap(routers.get)
          .foldLeft(0)((expectedReplies, router) => {
            router ! TaskRequestProtocol.CheckHealth(taskResponseMapper)
            expectedReplies + 1
          })
      }

      // Looking for the entry point Task then starts the health check process from there down the Graph
      g.nodes find (g having (node = _.value.isEntryPoint == true)) match {
        case Some(node) =>
          context.log.info("Health Check requested ...")
          context.self ! TaskManagerRequestProtocol.HealthCheckNextTask
          taskHealthCheck(g.outerNodeTraverser(node).map(_.value).iterator)

        case None =>
          context.log.error("No entry point Task found, cannot health check")
          manageRequests(taskResponseMapper, g, routers)
      }
    }

  /**
   * Creates a graph for the provided service deployment
   * @param services A list of services to deploy
   * @return The Graph of tasks for the provided service deployment
   */
  def buildServiceDeploymentGraph(services: List[ServiceDeployment]): TaskGraph =
    createEdges(services, createNodes(services))

  /**
   * Creates the Nodes of a Graph corresponding to a given list of services
   * @param services A list of services to deploy
   * @return The Graph of Noddes
   */
  def createNodes(services: List[ServiceDeployment]): TaskGraph = {
    val g = Graph.empty[Task, DiEdge]

    for {
      (service, idx) <- services.zipWithIndex
    } yield g += Task(idx, service.serviceName, service.entryPoint, service.replicas, service.dependencies)

    g
  }

  /**
   * Creates the Edges of a Graph corresponding to a given list of services
   * @param services A list of services to deploy
   * @param g The Graph containing the Nodes for the services to deploy
   */
  def createEdges(services: List[ServiceDeployment], g: TaskGraph): TaskGraph = {
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
  def spawnTasks(g: TaskGraph,
                 taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                 replyTo: ActorRef[TaskManagerResponseProtocol.Response])
                (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Behavior[TaskManagerRequestProtocol.Request] = {
    g.topologicalSort match {
      case Right(topology) =>
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
                     g: TaskGraph,
                     taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                     replyTo: ActorRef[TaskManagerResponseProtocol.Response],
                     routers: TaskRouters = Map())
                    (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.withTimers { timer =>

      def spawnNextTask(tasks: List[Task],
                        routers: TaskRouters): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
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
                                 router: TaskRouter,
                                 routers: TaskRouters): Behaviors.Receive[TaskManagerRequestProtocol.Request] =
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

      context.self ! TaskManagerRequestProtocol.SpawnNextTask
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
  def cleanUp(routers: TaskRouters)
             (implicit context: ActorContext[TaskManagerRequestProtocol.Request]): TaskRouters = {
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
   * Provides the topology of the Graph, this allows us to check that a Graph is valid
   * @param g A Graph containing Nodes/Edges for the services to deploy
   * @return A list of Nodes (Tasks)
   */
  def getTopology(g: TaskGraph): Try[List[g.NodeT]] = {
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
