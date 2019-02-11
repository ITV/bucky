package com.itv.bucky.fs2

import cats.effect.{ContextShift, IO}
import com.itv.bucky.fs2.utils._
import com.itv.bucky.suite.LoadPublishMessagesTest

import scala.language.higherKinds
import scala.concurrent.ExecutionContext.Implicits.global
import cats.implicits._

class IOLoadPublishMessagesTest
    extends LoadPublishMessagesTest[IO]
    with IOPublisherConsumerBaseTest
    with IOEffectVerification
    with IOEffectMonad {
//  override val numberRequestInParallel = 20000

  implicit val contextShift: ContextShift[IO] = IO.contextShift(implicitly)

  override def sequence[A](list: Seq[IO[A]]): IO[Seq[A]] = list.toList.parSequence
}
