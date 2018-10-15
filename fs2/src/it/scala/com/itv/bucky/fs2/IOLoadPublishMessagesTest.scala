package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.fs2.utils._
import com.itv.bucky.suite.LoadPublishMessagesTest
import _root_.fs2.async

import scala.language.higherKinds
import scala.concurrent.ExecutionContext.Implicits.global
import cats.implicits._

class IOLoadPublishMessagesTest
    extends LoadPublishMessagesTest[IO]
    with IOPublisherConsumerBaseTest
    with IOEffectVerification
    with IOEffectMonad {
//  override val numberRequestInParallel = 20000

  override def sequence[A](list: Seq[IO[A]]): IO[Seq[A]] = async.parallelSequence(list.toList)
}
