package com.itv.bucky

import java.util.concurrent.TimeUnit

import com.itv.lifecycle._
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext
import scala.util.Try

class RawAmqpClient(channelFactory: Lifecycle[Channel]) extends AmqpClient with StrictLogging {

  def consumer(queueName: QueueName, handler: Handler[Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0)
              (implicit executionContext: ExecutionContext): Lifecycle[Unit] =
    for {
      channel <- channelFactory
    } yield
      RabbitConsumer(queueName, handler, actionOnFailure, prefetchCount, channel)



  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]] =
    for {
      channel <- channelFactory
      publisherWrapper <- publisherWrapperLifecycle(timeout)
    } yield
      RabbitPublisher(channel, publisherWrapper)



  private def publisherWrapperLifecycle(timeout: Duration): Lifecycle[Publisher[PublishCommand] => Publisher[PublishCommand]] = timeout match {
    case finiteTimeout: FiniteDuration =>
      ExecutorLifecycles.singleThreadScheduledExecutor.map(ec => new TimeoutPublisher(_, finiteTimeout)(ec))
    case _ => NoOpLifecycle(identity)
  }


  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] =
    Lifecycle.using(channelFactory)(channel => thunk(ChannelAmqpOps(channel)))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] =
    Try(Lifecycle.using(channelFactory) { channel =>
      Option(channel.basicGet(queueName.value, false)).fold(0)(_.getMessageCount + 1)
    })
}