package com.itv.bucky.stream

import com.itv.bucky.Monad.Id
import com.itv.bucky.future.FutureIdAmqpClient
import com.itv.bucky.{MessagePropertiesConverters, _}
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.Try
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream._


case class StreamAmqpClient(channel: Id[Channel]) extends AmqpClient[Id, Task, Throwable, Task[Unit]] with StrictLogging {
  override def publisher(timeout: Duration): Id[Publisher[Task, PublishCommand]] = cmd => Task {
    channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, MessagePropertiesConverters(cmd.basicProperties), cmd.body.value)
  }

  override def consumer(queueName: QueueName, handler: Handler[Task, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): Id[Task[Unit]] = {
    Task.async { register =>
      IdConsumer[Task, Throwable](channel, queueName, handler, actionOnFailure) { result =>
        register(\/-(result.unsafePerformSync))
      }
    }
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = IdChannel.estimateMessageCount(channel, queueName)
}


object StreamAmqpClient extends StrictLogging {
  import Monad._

  def apply(config: AmqpClientConfig): StreamAmqpClient = IdConnection(config).flatMap(
    IdChannel(_)
  ).flatMap(
    StreamAmqpClient(_)
  )

}