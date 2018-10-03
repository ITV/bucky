package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.decl.Declaration
import com.itv.lifecycle.Lifecycle
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging
import com.rabbitmq.client.{
  DefaultConsumer,
  Channel => RabbitChannel,
  Consumer => RabbitMqConsumer,
  Envelope => RabbitEnvelope
}

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

object IOAmqpClient extends StrictLogging {
  import com.itv.bucky.Monad._
  import _root_.fs2._

  def apply(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext): IOAmqpClient =
    new IOAmqpClient {
      override implicit def monad: Monad[Id] = Monad.idMonad

      override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError

      override def publisher(timeout: Duration): Id[Publisher[IO, PublishCommand]] = {
        logger.info(s"Creating publisher")
        val handleFailure    = (f: Register, e: Exception) => f.apply(Left(e))
        val channelPublisher = ChannelPublisher(channel)
        val pendingConfirmations = channelPublisher.confirmListener[Register] {
          _.apply(Right(()))
        }(handleFailure)

        cmd =>
          val publishing = IO.async { pendingConfirmation: Register =>
            channelPublisher.publish[Register](cmd, pendingConfirmation, pendingConfirmations)(handleFailure)
          }
          timeout match {
            case fd: FiniteDuration =>
              Scheduler[IO](2)
                .flatMap { implicit s =>
                  Stream.eval(
                    publishing
                      .timed(fd))
                }
                .attempt
                .compile
                .last
                .flatMap(_.fold(IO.raiseError[Unit](new TimeoutException(s"Timed out after $fd")))(
                  _.fold(IO.raiseError, _ => IO.unit)))
            case _ => publishing
          }

      }

      import _root_.fs2.async._
      override def consumer(queueName: QueueName,
                            handler: Handler[IO, Delivery],
                            actionOnFailure: ConsumeAction = DeadLetter,
                            prefetchCount: Int = 0): IOConsumer =
        for {
          messages <- Stream.eval(async.unboundedQueue[IO, Delivery])
          buildConsumer = Stream eval IO {
            val consumer = IOConsumer(channel, queueName, messages)
            Consumer[IO, Throwable](channel, queueName, consumer, prefetchCount)(ioMonadError)
            consumer
          }
          _ <- buildConsumer
          process <- messages.dequeue to Sink { delivery =>
            Consumer.processDelivery(channel, queueName, handler, actionOnFailure, delivery)(ioMonadError)
          }

        } yield process

      def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

      def estimatedMessageCount(queueName: QueueName): Try[Int] =
        Channel.estimateMessageCount(channel, queueName)
    }

  def lifecycle(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): Lifecycle[IOAmqpClient] =
    for {
      connection <- Connection.lifecycle(IOConnection(config).unsafeRunSync())
      channel    <- Channel.lifecycle(connection)
    } yield IOAmqpClient(channel)

  def use[O](config: AmqpClientConfig, declarations: List[Declaration] = List.empty)(f: IOAmqpClient => Stream[IO, O])(
      implicit executionContext: ExecutionContext): Stream[IO, O] =
    for {
      client <- clientFrom(config, declarations)
      p      <- f(client)
    } yield p

  private object IOConsumer extends StrictLogging {
    import _root_.fs2.async.mutable.Queue

    def apply(channel: Id[RabbitChannel], queueName: QueueName, messages: Queue[IO, Delivery]): RabbitMqConsumer =
      new DefaultConsumer(channel) {
        logger.info(s"Creating consumer for $queueName")
        override def handleDelivery(consumerTag: String,
                                    envelope: RabbitEnvelope,
                                    properties: BasicProperties,
                                    body: Array[Byte]): Unit = {
          val delivery = Consumer.deliveryFrom(consumerTag, envelope, properties, body)
          messages
            .enqueue1(delivery)
            .attempt
            .map(_.fold(
              exception => {
                logger.error(s"Not able to enqueue $delivery because ${exception.getMessage}", exception)
                Consumer.requeueImmediately(channel, delivery)
              },
              identity
            ))
            .unsafeRunSync()

        }
      }
  }

}
