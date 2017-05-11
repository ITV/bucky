package com.itv.bucky.task

import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.Try
import scalaz.{-\/, \/, \/-}
import scalaz.concurrent.Task

case class TaskAmqpClient(channel: Id[Channel]) extends AmqpClient[Id, Task, Throwable, Task[Unit]] with StrictLogging {

  type Register = (\/[Throwable, Unit]) => Unit

  override def publisher(timeout: Duration): Id[Publisher[Task, PublishCommand]] = {
    logger.info(s"Creating publisher")
    val handleFailure = (f: Register, e: Exception) => f.apply(-\/(e))
    val pendingConfirmations = IdChannel.confirmListener[Register](channel) {
      _.apply(\/-(()))
    }(handleFailure)

    cmd =>
      Task.async { pendingConfirmation: Register =>
        IdPublisher.publish[Register](channel, cmd, pendingConfirmation, pendingConfirmations)(handleFailure)
      }
  }

  override def consumer(queueName: QueueName, handler: Handler[Task, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): Id[Task[Unit]] =
    Task.async {
      register =>
        IdConsumer[Task, Throwable](channel, queueName, handler, actionOnFailure) {
          result =>
            register(\/-(result.unsafePerformSync))
        }
    }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = IdChannel.estimateMessageCount(channel, queueName)

}


object TaskAmqpClient extends StrictLogging {
  import Monad._

  def apply(config: AmqpClientConfig): TaskAmqpClient = IdConnection(config).flatMap(
    IdChannel(_)
  ).flatMap(
    TaskAmqpClient(_)
  )
}
