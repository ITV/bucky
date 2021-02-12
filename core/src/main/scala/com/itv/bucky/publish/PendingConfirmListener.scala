package com.itv.bucky.publish

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import cats.instances.list._
import com.rabbitmq.client.ConfirmListener
import org.typelevel.log4cats.Logger

import scala.collection.compat._
import scala.collection.immutable.TreeMap
import scala.language.higherKinds

private[bucky] case class PendingConfirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]])(
    implicit F: ConcurrentEffect[F], logger: Logger[F])
    extends ConfirmListener {

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
      toComplete <- pop(deliveryTag, multiple)
      _          <- logger.info(s"Received ack for delivery tag: $deliveryTag and multiple: $multiple")
      _          <- toComplete.traverse(_.complete(true))
    } yield ()).toIO
      .unsafeRunSync()

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      toComplete <- pop(deliveryTag, multiple)
      _          <- logger.error(s"Received Nack for delivery tag: $deliveryTag and multiple: $multiple")
      _          <- toComplete.traverse(_.complete(false))
    } yield ()).toIO
      .unsafeRunSync()
}
