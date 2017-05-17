package com.itv.bucky.future

import scala.concurrent.{Future, Promise}
import scala.util.Try

object FutureExt {

  implicit val executionContext = SameThreadExecutionContext.implicitly

  implicit val theFutureMonad = futureMonad(executionContext)

  implicit class FutureOps[T](f: Future[T]) {
    def asTry: Future[Try[T]] = {
      val p = Promise[Try[T]]()
      f.onComplete(p.success)(SameThreadExecutionContext)
      p.future
    }
  }
}
