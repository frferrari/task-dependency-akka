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

  class TaskNotDeployedException(message: String) extends Exception(message)

  class InvalidServiceSpecificationException(message: String) extends Exception(message)

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

  def manageRequests(taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                     g: Graph[Task, DiEdge] = Graph.empty[Task, DiEdge],
                     routers: Map[String, ActorRef[TaskRequestProtocol.Request]] = Map.empty[String, ActorRef[TaskRequestProtocol.Request]]): Behavior[TaskManagerRequestProtocol.Request] =
    Behaviors.receive { (context, message) =>
      message match {
        case TaskManagerRequestProtocol.Deploy(services) =>
          deploy(services)(context) match {
            case Success((newGraph, routers)) =>
              context.log.info("All Tasks successfully deployed")
              manageRequests(taskResponseMapper, newGraph, routers)
            case Failure(e) =>
              context.log.error("Error encountered while deploying the Tasks: " + e.getMessage)
              manageRequests(taskResponseMapper, g, routers)
          }

        case TaskManagerRequestProtocol.CheckHealth =>
          getTopology(g)(context) match {
            case Success(topology) =>
              // Send a CheckHealth message to each Task Router
              for {
                task <- topology.map(_.value)
                router <- routers.get(task.name)
              } yield {
                router ! TaskRequestProtocol.CheckHealth(taskResponseMapper)
              }
              tasksHealthChecking(taskResponseMapper, g, routers, topology.size)

            case Failure(e) =>
              context.log.error(e.getMessage)
              Behaviors.same
          }
      }
    }

  def tasksHealthChecking(taskResponseMapper: ActorRef[TaskResponseProtocol.Response],
                          g: Graph[Task, DiEdge],
                          routers: Map[String, ActorRef[TaskRequestProtocol.Request]],
                          taskCount: Int): Behavior[TaskManagerRequestProtocol.Request] = {
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
                  manageRequests(taskResponseMapper, g, routers)
                } else {
                  timer.startSingleTimer(TaskManagerRequestProtocol.HealthCheckTimeout, 3.seconds)
                  tasksHealthChecking(taskResponseMapper, g, routers, newTaskCount)
                }
            }

          case TaskManagerRequestProtocol.HealthCheckTimeout =>
            context.log.error("A HealthCheck Timeout has been received, as least one service is not healthy")
            manageRequests(taskResponseMapper, g, routers)
        }
      }
    }
  }

  def deploy(services: List[ServiceDeployment])(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Try[(Graph[Task, DiEdge], Map[String, ActorRef[TaskRequestProtocol.Request]])] = {
    val g = Graph.empty[Task, DiEdge]

    // Create the Nodes and the Edges
    createNodes(services, g)
    createEdges(services, g)

    // Spawn the Tasks if the deployment specification is not cyclic
    if (g.isCyclic)
      Failure(new IllegalArgumentException("The service deployment specification is cyclic"))
    else
      spawnTasks(g)
  }

  def createNodes(services: List[ServiceDeployment], g: Graph[Task, DiEdge]): Unit =
    for {
      service <- services
    } yield g += Task(name = service.serviceName, isEntryPoint = service.entryPoint, replicas = service.replicas)

  def createEdges(services: List[ServiceDeployment], g: Graph[Task, DiEdge]): Unit = {
    def nodeSelection(lookupNodeName: String)(node: g.NodeT): Boolean = node == lookupNodeName

    for {
      service <- services
      dependency <- service.dependencies
      parent <- (g.nodes find (g having (node = nodeSelection(service.serviceName)))).map(_.value)
      children <- (g.nodes find (g having (node = nodeSelection(dependency)))).map(_.value)
    } yield {
      g += parent ~> children
    }
  }

  def spawnTasks(g: Graph[Task, DiEdge])(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Try[(Graph[Task, DiEdge], Map[String, ActorRef[TaskRequestProtocol.Request]])] = {
    g.topologicalSort match {
      case Right(topology) =>
        val routers = topology.toList.reverse.foldLeft(Map.empty[String, ActorRef[TaskRequestProtocol.Request]]) {
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

  def getTopology(g: Graph[Task, DiEdge])(implicit context: ActorContext[TaskManagerRequestProtocol.Request]): Try[List[g.NodeT]] = {
    g.topologicalSort match {
      case Right(topology) =>
        topology.toList match {
          case topo@h :: t =>
            Success(topo)
          case _ =>
            Failure(new InvalidServiceSpecificationException("The graph topology is empty"))
        }
      case Left(_) =>
        Failure(new InvalidServiceSpecificationException("Could not generate the graph topology due to an invalid service specification"))
    }
  }
}
