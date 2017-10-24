package com.itv.bucky

import java.util.concurrent.TimeoutException

import cats.effect.IO
import _root_.fs2._
import com.itv.bucky.Monad.Id

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

package object fs2 {
  type IOAmqpClient = AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]

  type Register = (Either[Throwable, Unit]) => Unit

  type IOConsumer = Id[Stream[IO, Unit]]

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
    import AtomicRef._
    def timed(duration: Duration)(implicit executionContext: ExecutionContext): IO[A] = duration match {
      case finiteDuration: FiniteDuration =>
        val scheduleTimeout =
          Scheduler[IO](corePoolSize = 1).flatMap { scheduler =>
            scheduler.sleep[IO](finiteDuration)
          }.runLast

        async
          .race(scheduleTimeout, io)
          .flatMap {
            _.fold(
              _ => IO.raiseError(new TimeoutException(s"Timed out after $duration")),
              s => IO.pure(s)
            )
          }
      case _ => io
    }

    def retry(delay: FiniteDuration,
              nextDelay: FiniteDuration => FiniteDuration,
              maxRetries: Int,
              retriable: Throwable => Boolean = internal.NonFatal.apply)(implicit executionContext: ExecutionContext) =
      Scheduler
        .apply[IO](1)
        .flatMap(_.retry(io, delay, nextDelay, maxRetries, retriable))
        .runLast
  }

}
