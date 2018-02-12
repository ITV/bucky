package com.itv.bucky.fs2

import _root_.fs2._
import cats.effect.IO
import com.itv.bucky.future.SameThreadExecutionContext.implicitly

object Foo extends App {

  Stream
    .eval(IO(println("foo")))
    .mergeHaltL(Stream.eval(IO(println("bar"))).repeat)
    .compile
    .last
    .unsafeRunSync()

}
