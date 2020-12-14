package com.fferrari.model

trait Microservice {
  def isEntryPoint: Boolean
  def dependencies: List[String]
}

