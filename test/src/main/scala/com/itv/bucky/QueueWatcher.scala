package com.itv.bucky

import itv.bucky.{Ack, ConsumeAction, Handler}

import scala.concurrent.{Future, Promise}

class QueueWatcher[T] extends Handler[T] {

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
