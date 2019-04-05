package com.itv.bucky.test.stubs

import cats.effect.Sync
import com.itv.bucky.Publisher
import cats.implicits._
import scala.collection.mutable.ListBuffer
import scala.language.higherKinds

class StubPublisher[F[_], T](implicit F: Sync[F]) extends Publisher[F, T] {
  private val published: ListBuffer[T] = ListBuffer[T]()
  def recordedMessages: List[T]        = published.synchronized(published.toList)
  override def apply(v1: T): F[Unit]   = F.delay(published.synchronized(published += v1)).void
}
