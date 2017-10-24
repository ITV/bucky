package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.fs2.utils.{IOEffectVerification, IOPublisherBaseTest}
import com.itv.bucky.suite.EstimatedMessageCountTest
import org.scalatest.FunSuite

class IOEstimatedMessageCountTest
    extends FunSuite
    with EstimatedMessageCountTest[IO]
    with IOPublisherBaseTest
    with IOEffectVerification {
  import _root_.fs2.async
  import cats.implicits._
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  override def runAll(tasks: Seq[IO[Unit]]): Unit =
    async.parallelSequence(tasks.toList).unsafeRunSync()

}
