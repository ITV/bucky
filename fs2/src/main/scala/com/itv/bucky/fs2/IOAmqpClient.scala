package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.lifecycle.Lifecycle
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging
import com.rabbitmq.client.{
  DefaultConsumer,
  Channel => RabbitChannel,
  Connection => RabbitConnection,
  Consumer => RabbitMqConsumer,
  Envelope => RabbitEnvelope
}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

object IOAmqpClient extends StrictLogging {
  import com.itv.bucky.Monad._
  import _root_.fs2._
  import _root_.fs2.async.mutable._
  import cats.implicits._

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

  def use[O](config: AmqpClientConfig)(f: IOAmqpClient => Stream[IO, O])(
      implicit executionContext: ExecutionContext): Stream[IO, O] =
    for {
      halted <- Stream.eval(async.signalOf[IO, Boolean](false))
      amqpClientAndRequestShutDown <- Stream.bracket(IOConnection(config))(
        connnection => streamIOAmqpClient(connnection, halted),
        connection =>
          for {
            _ <- IO {
              logger.info(s"Closing connection ...")
              Connection.close(connection)
            }
            _ <- halted.set(true)
          } yield ()
      )
      (client, requestShutdown) = amqpClientAndRequestShutDown
      result <- f(client).interruptWhen(requestShutdown)
    } yield result

  private def streamIOAmqpClient(connnection: Id[RabbitConnection], halted: Signal[IO, Boolean])(
      implicit executionContext: ExecutionContext) =
    Stream.bracket(IO(Channel(connnection)))(
      channel => {
        logger.info(s"Using connection $channel")
        Stream.eval(for {
          requestShutdown <- async.signalOf[IO, Boolean](false)
          _               <- addShutdownHook(requestShutdown, halted)
        } yield IOAmqpClient(channel) -> requestShutdown)

      },
      channel =>
        IO {
          logger.info(s"Closing channel ...")
          Channel.close(channel)
      }
    )

  private def addShutdownHook(requestShutdown: Signal[IO, Boolean], halted: Signal[IO, Boolean]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        val hook = requestShutdown.set(true).runAsync(_ => IO.unit) >>
          halted.discrete
            .takeWhile(_ == false)
            .run
        hook.unsafeRunSync()
      }
      ()
    }

}

private object IOConnection {
  def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): IO[RabbitConnection] =
    config.networkRecoveryIntervalOnStart.fold(IO(Connection(config))) { c =>
      IO(Connection(config))
        .retry(c.interval, _ => c.interval, c.numberOfRetries.toInt, _ => true)
        .flatMap { f =>
          IO(f.get)
        }
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
