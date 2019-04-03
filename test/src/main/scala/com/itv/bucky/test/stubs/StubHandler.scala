package com.itv.bucky.test.stubs

import cats.effect.Sync
import cats._
import cats.implicits._
import com.itv.bucky.Handler
import com.itv.bucky.consume.{ConsumeAction}

import scala.collection.mutable.ListBuffer
import scala.language.higherKinds

class StubHandler[F[_], T](response: ConsumeAction)(implicit F: Sync[F]) extends Handler[F, T] {
  private val receivedMessages: ListBuffer[T] = ListBuffer[T]()
  val messagesReceived: List[T]               = receivedMessages.synchronized(receivedMessages.toList)
  override def apply(message: T): F[ConsumeAction] =
    F.delay(messagesReceived.synchronized(receivedMessages += message)) *> F.delay(response)
}
