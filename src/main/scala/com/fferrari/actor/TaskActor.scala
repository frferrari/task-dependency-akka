package com.fferrari.actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.fferrari.actor.protocol.{TaskRequestProtocol, TaskResponseProtocol}

object TaskActor {
  def apply(): Behavior[TaskRequestProtocol.Request] = Behaviors.setup[TaskRequestProtocol.Request] { context =>
    context.log.info(s"Starting Task ${context.self.path}")

    Behaviors.receiveMessage[TaskRequestProtocol.Request] {
      case TaskRequestProtocol.CheckHealth(replyTo) =>
        context.log.info(s"Handling CheckHealth request for Task ${context.self.path}")
        replyTo ! TaskResponseProtocol.TaskIsHealthy
        Behaviors.same

      case TaskRequestProtocol.Stop =>
        Behaviors.stopped
    }
  }
}
