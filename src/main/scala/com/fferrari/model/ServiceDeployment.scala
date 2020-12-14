package com.fferrari.model

import spray.json._

case class ServiceDeployment(serviceName: String,
                             entryPoint: Boolean,
                             replicas: Int,
                             dependencies: List[String])

trait ServiceDeploymentJsonProtocol extends DefaultJsonProtocol {
  implicit val serviceDeploymentFormat = jsonFormat4(ServiceDeployment)
}