package com.itv.bucky

import java.util.concurrent.{Executors, TimeUnit}

import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.higherKinds

abstract class RawAmqpClient[M[_]](channelFactory: M[Channel])(implicit M: Monad[M]) extends AmqpClient[M] with StrictLogging {

  def consumer(queueName: QueueName, handler: Handler[Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0)
              (implicit executionContext: ExecutionContext): M[Unit] =
    M.flatMap(channelFactory) { (channel: Channel) =>
      M.apply(IdConsumer(queueName, handler, actionOnFailure, prefetchCount, channel))
    }


  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): M[Publisher[PublishCommand]] =
    M.flatMap(channelFactory) { channel: Channel =>
      M.apply(
        IdPublisher(channel,publisherWrapperLifecycle(timeout))
      )
    }

  private def publisherWrapperLifecycle(timeout: Duration): Publisher[PublishCommand] => Publisher[PublishCommand] = timeout match {
      case finiteTimeout: FiniteDuration =>
        new TimeoutPublisher(_, finiteTimeout)(Executors.newSingleThreadScheduledExecutor())
      case _ => identity
    }
}
