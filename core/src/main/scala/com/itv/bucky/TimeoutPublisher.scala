package com.itv.bucky

import java.util.concurrent.{ScheduledExecutorService, TimeoutException}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

class TimeoutPublisher(delegate: Publisher[PublishCommand],
                       timeout: FiniteDuration)
                      (ec: ScheduledExecutorService) extends Publisher[PublishCommand] {
  override def apply(cmd: PublishCommand): Future[Unit] = {
    val promise = Promise[Unit]
    ec.schedule(
      new Runnable {
        override def run(): Unit = {
          promise.failure(new TimeoutException(s"Timed out after $timeout waiting for AMQP server to acknowledge publication to ${cmd.description}"))
        }
      },
      timeout.length,
      timeout.unit
    )
    delegate(cmd).onComplete(result =>
      promise.complete(result)
    )(ExecutionContext.fromExecutor(ec))

    promise.future
  }
}
