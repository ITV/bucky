package com.itv.bucky

import scala.concurrent.{Future, Promise}
import scala.language.higherKinds

class QueueWatcher[T] extends Handler[Future, T] {

  private val messages = Stream.continually(Promise[T]())

  private val reader: Iterator[Future[T]] = messages.iterator.map(_.future)
  private val writer: Iterator[Promise[T]] = messages.iterator

  def nextMessage() = reader.next()

  def receivedMessages: List[T] = messages.map(_.future.value).takeWhile(_.isDefined).map(_.get.get).toList

  override def apply(payload: T): Future[ConsumeAction] = {
    writer.next().success(payload)
    Future.successful(Ack)
  }

}
