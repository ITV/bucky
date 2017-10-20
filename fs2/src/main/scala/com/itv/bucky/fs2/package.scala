package com.itv.bucky

import cats.effect.IO
import _root_.fs2._
import com.itv.bucky.Monad.Id
import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
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
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

package object fs2 {
  type Register = (Either[Throwable, Unit]) => Unit

  type IOConsumer = Id[Stream[IO, Unit]]

  type IOAmqpClient = AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]

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
    import _root_.fs2._
    import _root_.fs2.async.mutable._
    import cats.implicits._

    private def connection(config: AmqpClientConfig)(
        implicit executionContext: ExecutionContext): IO[RabbitConnection] =
      config.networkRecoveryIntervalOnStart.fold(IO(Connection(config))) { c =>
        IO(Connection(config))
          .retry(c.interval, _ => c.interval, c.numberOfRetries.toInt, _ => true)
          .flatMap { f =>
            IO(f.get)
          }
      }

    def use[O](config: AmqpClientConfig)(f: IOAmqpClient => Stream[IO, O])(
        implicit executionContext: ExecutionContext): Stream[IO, O] = {
      val halted = async.signalOf[IO, Boolean](false).unsafeRunSync()
      val p = Stream.bracket(connection(config))(
        connnection => channel(connnection, halted),
        connection =>
          IO {
            logger.info(s"Closing connection ...")
            Connection.close(connection)
            halted.set(true).unsafeRunSync()
        }
      )

      p.flatMap { foo: IO[(IOAmqpClient, Signal[IO, Boolean])] =>
        Stream.eval(foo).flatMap {
          case (client, requestShutdown) =>
            f(client).interruptWhen(requestShutdown)
        }
      }
    }

    private def channel(connnection: Id[RabbitConnection], halted: Signal[IO, Boolean])(
        implicit executionContext: ExecutionContext) = {
      def addShutdownHook(requestShutdown: Signal[IO, Boolean], halted: Signal[IO, Boolean]): IO[Unit] =
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
      Stream.bracket(IO(Channel(connnection)))(
        channel => {
          logger.info(s"Using connection $channel")
          Stream.emit(for {
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
    }

    def lifecycle(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): Lifecycle[IOAmqpClient] =
      for {
        connection <- Connection.lifecycle(connection(config).unsafeRunSync())
        channel    <- Channel.lifecycle(connection)
      } yield IOAmqpClient(channel)

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

  implicit class IOExt[A](value: IO[A]) {

    def retry(delay: FiniteDuration,
              nextDelay: FiniteDuration => FiniteDuration,
              maxRetries: Int,
              retriable: Throwable => Boolean = internal.NonFatal.apply)(implicit executionContext: ExecutionContext) =
      Scheduler
        .apply[IO](1)
        .flatMap(_.retry(value, delay, nextDelay, maxRetries, retriable))
        .runLast
  }

}
