package com.itv.bucky

import com.itv.bucky.future.SameThreadExecutionContext

import scala.concurrent.{Future, Promise}
import scala.util.Try

object FutureExt {

  implicit class FutureOps[T](f: Future[T]) {
    def asTry: Future[Try[T]] = {
      val p = Promise[Try[T]]()
      f.onComplete(p.success)(SameThreadExecutionContext)
      p.future
    }
  }
}
