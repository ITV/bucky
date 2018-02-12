package com.itv.bucky.ext

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Try

package object future {

  implicit class FutureOps[T](f: Future[T]) {
    def asTry(implicit executionContext: ExecutionContext): Future[Try[T]] = {
      val p = Promise[Try[T]]()
      f.onComplete(p.success)(executionContext)
      p.future
    }

    def timed(implicit duration: FiniteDuration) = Await.result(f, duration)
  }
}
