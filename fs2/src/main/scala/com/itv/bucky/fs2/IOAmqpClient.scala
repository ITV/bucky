package com.itv.bucky.fs2

import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.Deferred
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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import _root_.fs2.concurrent.Queue
import com.itv.bucky

object IOAmqpClient extends StrictLogging {
  import com.itv.bucky.Monad._
  import _root_.fs2._

  case class PublishRequest(command: PublishCommand, deferred: Deferred[IO, IO[Unit]])

  val handleFailure = (f: Register, e: Exception) => f.apply(Left(e))

  def apply(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext): IOAmqpClient =
    IOAmqpClient.io(channel).unsafeRunSync()

  def io(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext): IO[IOAmqpClient] = {

    val channelPublisher = ChannelPublisher(channel)
    val pendingConfirmations = channelPublisher.confirmListener[Register] {
      _.apply(Right(()))
    }(handleFailure)

    for {
      publishQueue <- Queue.bounded[IO, PublishRequest](10)
      _ <- publishQueue.dequeue
        .evalMap { request =>
          request.deferred.complete(IO.async { pendingConfirmation: Register =>
            channelPublisher.publish[Register](request.command, pendingConfirmation, pendingConfirmations)(
              handleFailure)
          })
        }
        .attempt
        .compile
        .drain

    } yield
      new IOAmqpClient {
        override implicit def monad: Monad[Id] = Monad.idMonad

        override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError

        override def publisher(timeout: Duration): Id[Publisher[IO, PublishCommand]] =
          cmd =>
            for {
              promise <- Deferred[IO, IO[Unit]]
              _       <- publishQueue.enqueue1(PublishRequest(cmd, promise))
              _ <- timeout match {
                case d: FiniteDuration =>
                  promise.get.flatMap(identity).timed(d)(IO.timer(executionContext), implicitly)
                case _ => promise.get.flatMap(identity)
              }

            } yield ()

        override def consumer(queueName: QueueName,
                              handler: Handler[IO, Delivery],
                              actionOnFailure: ConsumeAction = DeadLetter,
                              prefetchCount: Int = 0): IOConsumer =
          for {
            messages <- Stream.eval[IO, Queue[IO, Delivery]](Queue.unbounded)
            buildConsumer = Stream.eval(IO {
              val consumer = IOConsumer(channel, queueName, messages)
              Consumer[IO, Throwable](channel, queueName, consumer, prefetchCount)(ioMonadError)
              consumer
            })
            _ <- buildConsumer
            process <- messages.dequeue.evalMap { delivery: Delivery =>
              Consumer.processDelivery(channel, queueName, handler, actionOnFailure, delivery)(ioMonadError)
            }
          } yield process

        def performOps(thunk: AmqpOps => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

        def estimatedMessageCount(queueName: QueueName): Try[Int] =
          Channel.estimateMessageCount(channel, queueName)
      }
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
