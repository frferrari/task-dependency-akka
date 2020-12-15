package com.fferrari

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import com.fferrari.model.{ServiceDeployment, ServiceDeploymentJsonProtocol}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

class NexthinkChallengeAppSpec
  extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceDeploymentJsonProtocol
    with SprayJsonSupport
    with NexthinkChallengeAppSpecFixture {

  import akka.actor.typed.scaladsl.adapter._

  implicit val typedSystem = system.toTyped
  implicit val timeout = Timeout(500.milliseconds)
  implicit val scheduler = system.scheduler

  "The deployment service" should {
    "return a BADREQUEST response for GET requests to /check when the service is not deployed" in {
      Get("/check") ~> NexthinkChallengeApp.routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "return a OK response for POST requests to /deploy with an acyclic deployment graph" in {
      Post("/deploy", List(serviceA, serviceB, serviceC)) ~> NexthinkChallengeApp.routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "return a OK response for GET requests to /check with a deployed service" in {
      Get("/check") ~> NexthinkChallengeApp.routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "return a InternalServerError response for POST requests to /deploy with a cyclic deployment graph" in {
      Post("/deploy", List(serviceA, serviceB, cyclicServiceC)) ~> NexthinkChallengeApp.routes ~> check {
        status shouldBe StatusCodes.InternalServerError
      }
    }
  }
}

trait NexthinkChallengeAppSpecFixture {
  val serviceA = ServiceDeployment("A", true, 1, List("B", "C"))
  val serviceB = ServiceDeployment("B", false, 2, List("C"))
  val serviceC = ServiceDeployment("C", false, 2, List())
  val cyclicServiceC = ServiceDeployment("C", false, 2, List("A"))
}
