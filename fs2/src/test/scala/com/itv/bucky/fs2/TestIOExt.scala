package com.itv.bucky.fs2

import cats.effect.IO

import scala.concurrent.ExecutionContext

object TestIOExt {

  import com.itv.bucky.AtomicRef._

  type IOResult[A] = Either[Throwable, A]

  implicit class TestIOEXt[A](io: IO[A]) {
    def status(implicit executionContext: ExecutionContext): IOStatus[A] = {
      import _root_.fs2.{io => _, _}
      val iOStatus = IOStatus(Ref[Option[IOResult[A]]](None))
      io.runAsync(r => IO(iOStatus.complete(r))).unsafeRunSync()
      iOStatus
    }
  }

  case class IOStatus[A](ref: Ref[Option[IOResult[A]]]) {

    import org.scalatest.Matchers.fail

    def complete(result: Either[Throwable, A]) = ref.set(Some(result))

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
