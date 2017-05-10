package com.itv.bucky.stream

import com.itv.bucky.Monad.Id
import com.itv.bucky.{MessagePropertiesConverters, _}
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.mutable.TreeMap
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.Try
import scalaz.{-\/, \/, \/-}
import scalaz.concurrent.Task


case class StreamAmqpClient(channel: Id[Channel]) extends AmqpClient[Id, Task, Throwable, Task[Unit]] with StrictLogging {

  type Register = (\/[Throwable, Unit]) => Unit

  override def publisher(timeout: Duration): Id[Publisher[Task, PublishCommand]] = {
    logger.info(s"Creating publisher")
    val pendingConfirmations = IdChannel.confirmListener[Register](channel) //
    {
      _.apply(\/-(()))
    } //
    {
      _.apply(-\/(new RuntimeException(s"AMQP server returned Nack for publications")))
    }

    (cmd: PublishCommand) => {
      for {
        deliveryTag <- Task {
          channel.synchronized {
            channel.getNextPublishSeqNo
          }
        }
        _ <- Task.taskInstance.both(Task.async { (register: (\/[Throwable, Unit]) => Unit) =>
          logger.debug(s"Waiting to be called for delivery tag: $deliveryTag")
          pendingConfirmations.addPendingConfirmation(deliveryTag, register)
        }.onFinish { _ =>
          Task {
            logger.debug(s"Message $deliveryTag has been delivered!")
          }
        },
          Task.delay {
            logger.debug("Publishing with delivery tag {}L to {}:{} with {}: {}", box(deliveryTag), cmd.exchange, cmd.routingKey, cmd.basicProperties, cmd.body)
            channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, MessagePropertiesConverters(cmd.basicProperties), cmd.body.value)
            deliveryTag
          }.handleWith {
            case exception =>
              logger.error(s"Failed to publish message with delivery tag ${deliveryTag}L to ${
                cmd.description
              }", exception)
              pendingConfirmations.completeConfirmation(deliveryTag)(_.apply(-\/(exception)))
              Task.fail(exception)
          })
      } yield ()

    }
  }

  override def consumer(queueName: QueueName, handler: Handler[Task, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): Id[Task[Unit]] = {
    Task.async {
      register =>
        IdConsumer[Task, Throwable](channel, queueName, handler, actionOnFailure) {
          result =>
            register(\/-(result.unsafePerformSync))
        }
    }
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = IdChannel.estimateMessageCount(channel, queueName)


  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]
}


object StreamAmqpClient extends StrictLogging {

  import Monad._

  def apply(config: AmqpClientConfig): StreamAmqpClient = IdConnection(config).flatMap(
    IdChannel(_)
  ).flatMap(
    StreamAmqpClient(_)
  )

}