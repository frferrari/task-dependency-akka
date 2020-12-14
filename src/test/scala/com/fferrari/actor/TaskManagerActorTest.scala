package com.fferrari.actor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import com.fferrari.actor.TaskManagerActor.{EmptyServiceSpecificationException, InvalidServiceSpecificationException}
import com.fferrari.actor.protocol.{TaskManagerRequestProtocol, TaskManagerResponseProtocol}
import com.fferrari.model.ServiceDeployment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

class TaskManagerActorTest
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with TaskManagerActorTestFixture {
  val testKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  it should "SUCCEED deploying a valid/acyclic service deployment" in {
    val services = List(serviceA, serviceB, serviceC)
    val taskManagerActor = testKit.spawn(TaskManagerActor(), "taskManager")
    val probe = testKit.createTestProbe[TaskManagerResponseProtocol.Response]()
    taskManagerActor ! TaskManagerRequestProtocol.Deploy(services, probe.ref)
    probe.expectMessage(TaskManagerResponseProtocol.DeploymentSuccessful)
    testKit.stop(taskManagerActor)
  }

  it should "SUCCEED checking the Health of a deployed service deployment" in {
    val services = List(serviceA, serviceB, serviceC)
    val taskManagerActor = testKit.spawn(TaskManagerActor(), "taskManager")
    val probe = testKit.createTestProbe[TaskManagerResponseProtocol.Response]()
    taskManagerActor ! TaskManagerRequestProtocol.Deploy(services, probe.ref)
    probe.expectMessage(TaskManagerResponseProtocol.DeploymentSuccessful)
    taskManagerActor ! TaskManagerRequestProtocol.CheckHealth(probe.ref)
    probe.expectMessage(TaskManagerResponseProtocol.HealthStatus(true))
    testKit.stop(taskManagerActor)
  }

  it should "FAIL deploying a cyclic service deployment" in {
    val services = List(serviceA, serviceB, cyclicServiceC)
    val taskManagerActor = testKit.spawn(TaskManagerActor(), "taskManager")
    val probe = testKit.createTestProbe[TaskManagerResponseProtocol.Response]()
    taskManagerActor ! TaskManagerRequestProtocol.Deploy(services, probe.ref)
    probe.expectMessage(TaskManagerResponseProtocol.DeploymentError)
    testKit.stop(taskManagerActor)
  }

  it should "create the appropriate Nodes from a given service deployment" in {
    val services = List(serviceA, serviceB, serviceC)
    val g = TaskManagerActor.createNodes(services)
    g.nodes should contain theSameElementsAs (List(taskA, taskB, taskC))
  }

  it should "create the appropriate Edges from a given service deployment" in {
    val services = List(serviceA, serviceB, serviceC)
    val g = TaskManagerActor.createEdges(services, TaskManagerActor.createNodes(services))
    g.edges.size shouldEqual 3
    assert(g.edges.forall {
      case edge if edge.from == taskA && edge.to == taskB =>
        true
      case edge if edge.from == taskA && edge.to == taskC =>
        true
      case edge if edge.from == taskB && edge.to == taskC =>
        true
      case _ =>
        false
    })
  }

  it should "SUCCEED to extract the topology from non cyclic Graph" in {
    val g = Graph(taskA ~> taskB, taskA ~> taskC, taskB ~> taskC)
    TaskManagerActor.getTopology(g).map(_ should contain theSameElementsAs (List(taskA, taskB, taskC)))
  }

  it should "FAIL to extract the topology from an empty graph" in {
    assert(TaskManagerActor.getTopology(Graph()).fold(_.isInstanceOf[EmptyServiceSpecificationException], _ => false))
  }

  it should "FAIL to extract the topology from an invalid graph" in {
    val g = Graph(taskA ~> taskB, taskA ~> taskC, taskB ~> taskC, taskC ~> taskA)
    assert(TaskManagerActor.getTopology(g).fold(_.isInstanceOf[InvalidServiceSpecificationException], _ => false))
  }
}

trait TaskManagerActorTestFixture {
  val serviceA = ServiceDeployment("A", true, 1, List("B", "C"))
  val serviceB = ServiceDeployment("B", false, 2, List("C"))
  val serviceC = ServiceDeployment("C", false, 2, List())
  val cyclicServiceC = ServiceDeployment("C", false, 2, List("A"))

  val taskA = TaskManagerActor.Task("A", true, 1)
  val taskB = TaskManagerActor.Task("B", false, 2)
  val taskC = TaskManagerActor.Task("C", false, 4)
}