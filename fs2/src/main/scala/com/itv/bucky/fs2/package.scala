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

}
