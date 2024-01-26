package com.itv.bucky.test.stubs

import cats.effect.Sync
import com.itv.bucky.{Publisher, PublisherWithHeaders}
import cats.implicits._

import scala.collection.mutable.ListBuffer

class StubPublisher[F[_], T](implicit F: Sync[F]) extends Publisher[F, T] {
  private val published: ListBuffer[T] = ListBuffer[T]()
  def recordedMessages: List[T]        = published.synchronized(published.toList)
  override def apply(v1: T): F[Unit]   = F.delay(published.synchronized(published += v1)).void
}

class StubPublisherWithHeaders[F[_], T](implicit F: Sync[F]) extends PublisherWithHeaders[F, T] {
  private val published: ListBuffer[(T, Map[String, AnyRef])] = ListBuffer[(T, Map[String, AnyRef])]()
  def recordedMessages: List[(T, Map[String, AnyRef])]        = published.synchronized(published.toList)

  override def apply(v1: T, v2: Map[String, AnyRef]): F[Unit] =
    F.delay(published.synchronized(published += ((v1, v2)))).void
}
