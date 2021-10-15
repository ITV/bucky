package com.itv.bucky.publish

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import cats.effect.concurrent.{Deferred, Ref}
import com.rabbitmq.client.{AMQP, ConfirmListener, ReturnListener}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.collection.immutable.TreeMap
import scala.collection.compat._

private[bucky] case class PendingConfirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]], pendingReturn: Ref[F, Boolean])(
  implicit F: ConcurrentEffect[F])
  extends ConfirmListener
    with ReturnListener
    with StrictLogging {

  def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, Boolean]]] =
    pendingConfirmations.modify { x =>
      if (multiple) {
        val entries = x.rangeUntil(deliveryTag + 1).toList
        (x -- entries.map { case (key, _) => key }, entries.map { case (_, value) => value })
      } else {
        (x - deliveryTag, x.get(deliveryTag).toList)
      }
    }

  override def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      returnRef <- pendingReturn.getAndSet(false)
      toComplete <- pop(deliveryTag, multiple)
      _ <- F.delay(logger.info("Received ack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
      _ <- toComplete.traverse(_.complete(!returnRef))
    } yield ()).toIO
      .unsafeRunSync()

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      toComplete <- pop(deliveryTag, multiple)
      _ <- F.delay(logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
      _ <- pendingReturn.set(false)
      _ <- toComplete.traverse(_.complete(false))
    } yield ()).toIO
      .unsafeRunSync()

  override def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
    pendingReturn.set(true).toIO.unsafeRunSync()
  }
}
