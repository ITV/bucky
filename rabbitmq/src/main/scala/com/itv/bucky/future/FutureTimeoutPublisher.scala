package com.itv.bucky.future

import java.util.concurrent.ScheduledExecutorService

import com.itv.bucky.{PublishCommand, Publisher}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.language.higherKinds

class FutureTimeoutPublisher(delegate: Publisher[Future, PublishCommand],
                             timeout: FiniteDuration)
                            (ec: ScheduledExecutorService) extends Publisher[Future, PublishCommand] {
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
