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

  case class TaskNotDeployedException(message: String) extends Exception(message)

  case class InvalidServiceSpecificationException(message: String) extends Exception(message)

  case class EmptyServiceSpecificationException(message: String) extends Exception(message)

  /*
   * A Task represents a Service and it is handled through Akka actors
   * We need specific equals/hashCode methods for the Graph to be properly handled
   */
  case class Task(name: String, isEntryPoint: Boolean, replicas: Int) {
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
                     routers: Map[String, ActorRef[TaskRequestProtocol.Request]] = Map.empty[String, ActorRef[TaskRequestProtocol.Request]]): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.receive { (context, message) =>
      message match {
        case TaskManagerRequestProtocol.Deploy(services, replyTo) =>
          deploy(services)(context) match {
            case Success((newGraph, routers)) =>
              context.log.info("All Tasks successfully deployed")
              replyTo ! TaskManagerResponseProtocol.DeploymentSuccessful
              manageRequests(taskResponseMapper, newGraph, routers)

            case Failure(e) =>
              context.log.error("Error encountered while deploying the Tasks: " + e.getMessage)
              replyTo ! TaskManagerResponseProtocol.DeploymentError
              manageRequests(taskResponseMapper, g, routers)
          }

        case TaskManagerRequestProtocol.CheckHealth(replyTo) =>
          getTopology(g) match {
            case Success(topology) =>
              // Send a CheckHealth message to each Task Router
              for {
                task <- topology.map(_.value)
                router <- routers.get(task.name)
              } yield {
                router ! TaskRequestProtocol.CheckHealth(taskResponseMapper)
              }
              tasksHealthChecking(taskResponseMapper, g, routers, topology.size, replyTo)

            case Failure(e) =>
              context.log.error(e.getMessage)
              Behaviors.same
          }
      }
    }

  /**
   * Handles the reply of each Task of the service deployment to the HealthCheck message sent to them.
   * It awaits maximum 3 seconds for each reply per Task :
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
      timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, 3.seconds)

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
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, 3.seconds)
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
   * Creates a graph for the service deployment and spawns all the Tasks for this graph, including their dependencies,
   * only if the graph is not cyclic.
   * @param services A list of services to deploy
   * @param context An actor context
   * @return The Graph of tasks and a Map of the Actors for this tasks
   */
  def deploy(services: List[ServiceDeployment])(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Try[(Graph[Task, DiEdge], Map[String, ActorRef[TaskRequestProtocol.Request]])] = {
    // Create the Nodes and the Edges
    val g = createNodes(services)
    createEdges(services, g)

    // Spawn the Tasks if the deployment specification is not cyclic
    if (g.isCyclic)
      Failure(new IllegalArgumentException("The service deployment specification is cyclic"))
    else
      spawnTasks(g)
  }

  /**
   * Creates the Nodes of a Graph corresponding to a given list of services
   * @param services A list of services to deploy
   * @return The Graph of Noddes
   */
  def createNodes(services: List[ServiceDeployment]): Graph[Task, DiEdge] = {
    val g = Graph.empty[Task, DiEdge]

    for {
      service <- services
    } yield g += Task(name = service.serviceName, isEntryPoint = service.entryPoint, replicas = service.replicas)

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
  def spawnTasks(g: Graph[Task, DiEdge])(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Try[(Graph[Task, DiEdge], Map[String, ActorRef[TaskRequestProtocol.Request]])] = {
    g.topologicalSort match {
      case Right(topology) =>
        val routers: Map[String, ActorRef[TaskRequestProtocol.Request]] =
          topology
            .toList
            .reverse
            .foldLeft(Map.empty[String, ActorRef[TaskRequestProtocol.Request]]) {
              case (acc, node) =>
                val task: Task = node.value
                val replicas = if (task.replicas <= 0) 1 else task.replicas
                context.log.info(s"Spawning task ${task.name} with ${replicas} replicas")
                val pool = Routers.pool(poolSize = replicas)(
                  Behaviors.supervise(TaskActor()).onFailure[Exception](SupervisorStrategy.restart))
                val router: ActorRef[TaskRequestProtocol.Request] = context.spawn(pool, task.name)
                // TODO: Improvement: we could check if the task has started (before spawning the next task)
                acc + (task.name -> router)
            }
        Success((g, routers))
      case Left(t) =>
        Failure(new InvalidServiceSpecificationException(s"Cannot spawn Tasks from an invalid topology $t"))
    }
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
