package com.itv.bucky.future

object Implicits {

  implicit val executionContext = SameThreadExecutionContext.implicitly

  implicit val theFutureMonad = futureMonad(executionContext)

}
