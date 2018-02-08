package com.itv.bucky

import java.util.concurrent.TimeoutException

import cats.effect.IO
import _root_.fs2._
import _root_.fs2.async.mutable._
import cats.implicits._
import com.itv.bucky.Monad.Id
import com.itv.bucky.decl.{Declaration, DeclarationExecutor}
import com.rabbitmq.client.{Channel => RabbitChannel, Connection => RabbitConnection}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

package object fs2 extends StrictLogging {
  type IOAmqpClient = AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]

  type Register = (Either[Throwable, Unit]) => Unit

  type IOConsumer = Id[Stream[IO, Unit]]

  def clientFrom(config: AmqpClientConfig, declarations: List[Declaration] = List.empty)(
      implicit executionContext: ExecutionContext): Stream[IO, IOAmqpClient] =
    for {
      halted          <- Stream.eval(async.signalOf[IO, Boolean](false))
      requestShutdown <- Stream.eval(async.signalOf[IO, Boolean](false))
      _               <- addShutdownHook(requestShutdown, halted)
      connection      <- connection(config, halted)
      channel         <- channel(connection)
      client          <- client(channel, halted).interruptWhen(requestShutdown)
      _               <- declare(declarations)(client)
    } yield client

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

    def timed(timeout: FiniteDuration)(implicit s: Scheduler, ec: ExecutionContext): IO[A] =
      for {
        promise <- async.promise[IO, Either[Throwable, A]]
        _       <- async.fork(io.attempt.flatMap(promise.complete))
        a <- promise
          .timedGet(timeout, s)
          .flatMap(
            _.fold[IO[A]](IO.raiseError(new TimeoutException(s"Timed out after $timeout")))(
              _.fold(IO.raiseError[A], a => IO(a))))
      } yield a

    def retry(delay: FiniteDuration,
              nextDelay: FiniteDuration => FiniteDuration,
              maxRetries: Int,
              retriable: Throwable => Boolean = internal.NonFatal.apply)(implicit executionContext: ExecutionContext) =
      Scheduler
        .apply[IO](1)
        .flatMap(_.retry(io, delay, nextDelay, maxRetries, retriable))
        .compile
        .last
  }

  def connection(config: AmqpClientConfig, halted: Signal[IO, Boolean])(
      implicit executionContext: ExecutionContext): Stream[IO, RabbitConnection] =
    for {
      connection <- Stream.bracket(IOConnection(config))(
        connection => Stream.emit(connection),
        connection =>
          for {
            _ <- IO {
              logger.info(s"Closing connection ...")
              Connection.close(connection)
            }
            _ <- halted.set(true)
          } yield ()
      )
    } yield connection

  def channel(connnection: Id[RabbitConnection])(
      implicit executionContext: ExecutionContext): Stream[IO, RabbitChannel] =
    Stream.bracket(IO(Channel(connnection)))(
      channel => Stream.emit(channel),
      channel =>
        IO {
          logger.info(s"Closing channel ...")
          Channel.close(channel)
      }
    )

  def client(channel: Id[RabbitChannel], halted: Signal[IO, Boolean])(
      implicit executionContext: ExecutionContext): Stream[IO, IOAmqpClient] =
    Stream.eval(for {
      _ <- IO { logger.info(s"Using connection $channel") }
    } yield IOAmqpClient(channel))

  def declare(declarations: List[Declaration])(amqpClient: IOAmqpClient) =
    Stream.eval(IO(DeclarationExecutor(declarations, amqpClient)))

  private def addShutdownHook(requestShutdown: Signal[IO, Boolean], halted: Signal[IO, Boolean]): Stream[IO, Unit] =
    Stream.eval(IO {
      sys.addShutdownHook {
        val hook = requestShutdown.set(true).runAsync(_ => IO.unit) >>
          halted.discrete
            .takeWhile(_ == false)
            .compile
            .drain
        hook.unsafeRunSync()
      }
      ()
    })

  object IOConnection {
    def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): IO[RabbitConnection] =
      config.networkRecoveryIntervalOnStart.fold(IO(Connection(config))) { c =>
        IO(Connection(config))
          .retry(c.interval, _ => c.interval, c.numberOfRetries.toInt, _ => true)
          .flatMap { f =>
            IO(f.get)
          }
      }
  }
}
