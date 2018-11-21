package com.itv.bucky

import scala.collection.mutable.ListBuffer
import scala.language.higherKinds

class StubPublisher[F[_]: Monad, A](implicit F: Monad[F]) extends Publisher[F, A] {

  val publishedEvents = ListBuffer[A]()

  private var nextResponse: F[Unit] = F.apply { () }

  def respondToNextPublishWith(expectedResult: F[Unit]) =
    nextResponse = expectedResult

  override def apply(event: A): F[Unit] = {
    F.flatMap(F.apply(publishedEvents += event)) {_ => nextResponse}
  }
}
