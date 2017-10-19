package com.itv.bucky

import cats.effect.IO
import _root_.fs2._
import com.itv.bucky.Monad.Id
import com.itv.lifecycle.VanillaLifecycle
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging
import com.rabbitmq.client.{
  DefaultConsumer,
  Channel => RabbitChannel,
  Consumer => RabbitMqConsumer,
  Envelope => RabbitEnvelope
}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

package object fs2 {
  type Register = (Either[Throwable, Unit]) => Unit

  type IOAmqpClient = AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]

  case class IOAmqpClientLifecycle(config: AmqpClientConfig)(implicit executionContext: ExecutionContext)
      extends VanillaLifecycle[IOAmqpClient]
      with StrictLogging {
    override def start(): IOAmqpClient = IOAmqpClient(config)

    override def shutdown(instance: IOAmqpClient): Unit = ()

  }

  implicit val ioMonadError = new MonadError[IO, Throwable] {
    override def raiseError[A](e: Throwable): IO[A] = IO.raiseError(e)

    override def handleError[A](fa: IO[A])(f: (Throwable) => IO[A]): IO[A] =
      fa.attempt
        .flatMap(_.fold(f, IO.pure))

    override def apply[A](a: => A): IO[A] = IO.apply(a)

    override def map[A, B](m: IO[A])(f: (A) => B): IO[B] = m.map(f)

    override def flatMap[A, B](m: IO[A])(f: (A) => IO[B]): IO[B] = m.flatMap(f)

  }

  object IOAmqpClient extends StrictLogging {
    import com.itv.bucky.Monad._

    def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): IOAmqpClient = apply {
      val connection = Connection(config)
      Channel(connection)
    }

    def apply(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext): IOAmqpClient =
      new IOAmqpClient {
        override implicit def monad: Monad[Id] = Monad.idMonad

        override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError

        override def publisher(timeout: Duration): Id[Publisher[IO, PublishCommand]] = {
          logger.info(s"Creating publisher")
          val handleFailure = (f: Register, e: Exception) => f.apply(Left(e))
          val pendingConfirmations = Publisher.confirmListener[Register](channel) {
            _.apply(Right(()))
          }(handleFailure)

          cmd =>
            IO.async { pendingConfirmation: Register =>
              Publisher.publish[Register](channel, cmd, pendingConfirmation, pendingConfirmations)(handleFailure)
            }

          // FIXME .timed(timeout)
        }

        import _root_.fs2.async._
        override def consumer(queueName: QueueName,
                              handler: Handler[IO, Delivery],
                              actionOnFailure: ConsumeAction = DeadLetter,
                              prefetchCount: Int = 0): Id[Stream[IO, Unit]] =
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
  }

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
