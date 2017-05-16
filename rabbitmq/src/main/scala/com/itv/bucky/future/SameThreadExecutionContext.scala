package com.itv.bucky.future

import scala.concurrent.ExecutionContextExecutor

/**
  * ExecutionContext that runs work immediately on the caller thread.
  * Useful in tests so that assertions can be made as soon as a method returns.
  *
  * Can be used either explicitly:
  * {{{
  *   val f = Future(12345)(SameThreadExecutionContext)
  * }}}
  *
  * or implicitly:
  * {{{
  *   import SameThreadExecutionContext.implicitly
  *
  *   val f = Future(12345)
  * }}}
  */
object SameThreadExecutionContext extends ExecutionContextExecutor {
  override def execute(runnable: Runnable): Unit = runnable.run()

  override def reportFailure(cause: Throwable): Unit = throw cause

  implicit def implicitly = this
}
