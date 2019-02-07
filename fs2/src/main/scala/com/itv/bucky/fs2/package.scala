package com.itv.bucky

import java.util.concurrent.{TimeoutException => JavaTimeoutException}

import cats.effect.{ContextShift, IO, Timer}
import _root_.fs2._
import _root_.fs2.concurrent.SignallingRef
import cats.effect.concurrent.Deferred
import com.itv.bucky.Monad.Id
import com.itv.bucky.decl.{Declaration, DeclarationExecutor}
import com.rabbitmq.client.{Channel => RabbitChannel, Connection => RabbitConnection}
import com.typesafe.scalalogging.StrictLogging
import cats.implicits._

import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import scala.util.control.NonFatal

package object fs2 extends StrictLogging {
  //Implicit conversion to get a contextShift from an ExecutionContext; needed to create a
  // ConcurrentEffect[IO] which gets used by Stream.eval and other methods
  implicit def toContextShift(implicit ec: ExecutionContext): ContextShift[IO] = IO.contextShift(ec)

  type IOAmqpClient = AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]

  type Register = Either[Throwable, Unit] => Unit

  type IOConsumer = Id[Stream[IO, Unit]]

  def clientFrom(config: AmqpClientConfig, declarations: List[Declaration] = List.empty)(
      implicit executionContext: ExecutionContext): Stream[IO, IOAmqpClient] =
    closeableClientFrom(config, declarations).map(_.client)

  def closeableClientFrom(config: AmqpClientConfig, declarations: List[Declaration] = List.empty)(
      implicit executionContext: ExecutionContext)
    : Stream[IO, AmqpClient.WithCloseable[Id, IO, Throwable, Stream[IO, Unit]]] =
    for {
      halted          <- Stream.eval(SignallingRef[IO, Boolean](false))
      requestShutdown <- Stream.eval(SignallingRef[IO, Boolean](false))
      _               <- addShutdownHook(requestShutdown, halted)
      connection      <- connection(config, halted)
      channel         <- channel(connection)
      client          <- client(channel).interruptWhen(requestShutdown)
      _               <- declare(declarations)(client)
    } yield AmqpClient.WithCloseable[Id, IO, Throwable, Stream[IO, Unit]](client, close(requestShutdown, halted))

  implicit val ioMonadError = new MonadError[IO, Throwable] {
    override def raiseError[A](e: Throwable): IO[A] = IO.raiseError(e)

    override def handleError[A](fa: IO[A])(f: (Throwable) => IO[A]): IO[A] =
      fa.attempt
        .flatMap(_.fold(f, IO.pure))

    override def apply[A](a: => A): IO[A] = IO.apply(a)

    override def map[A, B](m: IO[A])(f: (A) => B): IO[B] = m.map(f)

    override def flatMap[A, B](m: IO[A])(f: (A) => IO[B]): IO[B] = m.flatMap(f)

  }

  implicit class IOExt[A](io: IO[A]) {

    def retry(delay: FiniteDuration,
              nextDelay: FiniteDuration => FiniteDuration,
              maxRetries: Int,
              retriable: Throwable => Boolean = NonFatal.apply)(implicit executionContext: ExecutionContext) =
      Stream
        .retry(io, delay, nextDelay, maxRetries, retriable)(IO.timer(executionContext), implicitly)
        .compile
        .last
  }

  def connection(config: AmqpClientConfig, halted: SignallingRef[IO, Boolean])(
      implicit executionContext: ExecutionContext): Stream[IO, RabbitConnection] =
    for {
      connection <- Stream.bracket(IOConnection(config))(connection =>
        for {
          _ <- IO {
            logger.info(s"Closing connection ...")
            Connection.close(connection)
          }
          _ <- halted.set(true)
        } yield ())
    } yield connection

  def channel(connection: RabbitConnection)(implicit executionContext: ExecutionContext): Stream[IO, RabbitChannel] =
    Stream.bracket(IO(Channel(connection)))(channel =>
      IO {
        logger.info(s"Closing channel ...")
        Channel.close(channel)
    })

  def client(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext): Stream[IO, IOAmqpClient] =
    Stream.eval(for {
      _      <- IO(logger.info(s"Using connection $channel"))
      client <- IOAmqpClient.io(channel)
    } yield client)

  def declare(declarations: List[Declaration])(amqpClient: IOAmqpClient): Stream[IO, Unit] =
    Stream.eval(IO(DeclarationExecutor(declarations, amqpClient)))

  private def addShutdownHook(requestShutdown: SignallingRef[IO, Boolean],
                              halted: SignallingRef[IO, Boolean]): Stream[IO, Unit] =
    Stream.eval(IO {
      sys.addShutdownHook {
        close(requestShutdown, halted).unsafeRunSync()
      }
      ()
    })

  private def close(requestShutdown: SignallingRef[IO, Boolean], halted: SignallingRef[IO, Boolean]): IO[Unit] =
    for {
      _ <- requestShutdown.set(true).runAsync(_ => IO.unit).toIO
      _ <- halted.discrete
        .takeWhile(_ == false)
        .compile
        .drain
    } yield ()

  object IOConnection {
    def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): IO[RabbitConnection] =
      config.networkRecoveryIntervalOnStart.fold(IO(Connection(config))) { c =>
        IO(Connection(config))
          .retry(c.interval, _ => c.interval, c.numberOfRetries.toInt, _ => true)
          .flatMap { f => IO(f.get)
          }
      }
  }
}
