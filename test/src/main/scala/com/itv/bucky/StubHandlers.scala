package com.itv.bucky

import scala.collection.mutable.ListBuffer
import scala.language.higherKinds

class StubHandler[F[_], T, S](var nextResponse: F[S], var nextException: Option[Throwable] = None) extends (T => F[S]) {

  val receivedMessages = ListBuffer[T]()

  override def apply(message: T): F[S] = {
    receivedMessages += message
    nextException.fold[F[S]](nextResponse)(throw _)
  }

}

class StubConsumeHandler[F[_], T](implicit F: Monad[F]) extends StubHandler[F, T, ConsumeAction](F.apply(Ack))

class StubRequeueHandler[F[_], T](implicit F: Monad[F]) extends StubHandler[F, T, RequeueConsumeAction](F.apply(Ack))
