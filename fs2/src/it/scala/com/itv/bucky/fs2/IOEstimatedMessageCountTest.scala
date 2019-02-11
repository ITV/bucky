package com.itv.bucky.fs2

import cats.Parallel
import cats.effect.{Concurrent, ContextShift, IO}
import com.itv.bucky.fs2.utils.{IOEffectVerification, IOPublisherBaseTest}
import com.itv.bucky.suite.EstimatedMessageCountTest
import org.scalatest.FunSuite
import cats.effect.syntax._
import cats.effect.implicits._
import cats.data._
import cats.implicits._

class IOEstimatedMessageCountTest
    extends FunSuite
    with EstimatedMessageCountTest[IO]
    with IOPublisherBaseTest
    with IOEffectVerification {
  import cats.implicits._
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly


  implicit val contextShift: ContextShift[IO] = IO.contextShift(implicitly)

  override def runAll(tasks: Seq[IO[Unit]]): Unit =
    tasks.toList.sequence.unsafeRunSync()

}
