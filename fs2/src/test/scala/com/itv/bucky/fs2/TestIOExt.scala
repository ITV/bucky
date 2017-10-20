package com.itv.bucky.fs2

import java.util.concurrent.{Executor, ExecutorService, Executors}

import cats.effect.IO

import scala.concurrent.ExecutionContext

object TestIOExt {

  import com.itv.bucky.AtomicRef._

  type IOResult[A] = Either[Throwable, A]
  implicit class TestIOEXt[A](io: IO[A]) {
    def status: IOStatus = {
      import _root_.fs2._
      val status = IOStatus(Ref[Option[IOResult[Unit]]](None))

      implicit val foo = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

      async
        .unsafeRunAsync(io)(either => IO(status.complete(either.map(_ => ()))))

      status
    }
  }

  case class IOStatus(ref: Ref[Option[IOResult[Unit]]]) {
    import org.scalatest.Matchers.fail

    def complete(result: Either[Throwable, Unit]) = ref.set(Some(result))

    def isRunning = ref.get().isEmpty

    def isCompleted = ref.get().isDefined

    def isFailure = ref.get().get.isLeft

    def isSuccess: Boolean = ref.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Boolean](
        (e: Throwable) => fail(s"It should not fail"),
        _ => true
      )
    }

    def failure: Throwable = ref.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Throwable](
        identity,
        _ => fail("It should not be completed successfully")
      )
    }
  }

}
