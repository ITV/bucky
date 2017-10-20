package com.itv.bucky

import java.util.concurrent.TimeoutException

import cats.effect.IO
import _root_.fs2._
import com.itv.bucky.Monad.Id

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

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

  implicit class IOExt[A](io: IO[A]) {
    def timed(duration: Duration): IO[A] = {
      def timeout(message: String) = IO.raiseError(new TimeoutException(s"Timed out after $duration because $message"))
      IO {
        io.unsafeRunTimed(duration) //FIXME Implement a better implementation
      }.attempt.flatMap(
        _.fold(
          exception => timeout(exception.getMessage),
          _.fold[IO[A]](timeout("unable to execute the task"))(IO.pure)
        ))
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
