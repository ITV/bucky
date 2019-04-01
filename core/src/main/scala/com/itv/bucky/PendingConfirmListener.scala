package com.itv.bucky

import cats.effect.ConcurrentEffect
import cats.effect.concurrent.{Deferred, Ref}
import com.rabbitmq.client.ConfirmListener
import com.typesafe.scalalogging.StrictLogging
import scala.language.higherKinds
import scala.collection.immutable.TreeMap
import cats._
import cats.implicits._
import cats.effect.implicits._

private[bucky] case class PendingConfirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]])(
    implicit F: ConcurrentEffect[F])
    extends ConfirmListener
    with StrictLogging {

  def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, Boolean]]] =
    pendingConfirmations.modify { x =>
      if (multiple) {
        val entries = x.until(deliveryTag + 1).toList
        (x -- entries.map { case (key, _) => key }, entries.map { case (_, value) => value })
      } else {
        (x - deliveryTag, x.get(deliveryTag).toList)
      }
    }

  override def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      toComplete <- pop(deliveryTag, multiple)
      _          <- F.delay(logger.info("Received ack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
      _          <- toComplete.traverse(_.complete(true))
    } yield ()).toIO
      .unsafeRunSync()

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      toComplete <- pop(deliveryTag, multiple)
      _          <- F.delay(logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
      _          <- toComplete.traverse(_.complete(false))
    } yield ()).toIO
      .unsafeRunSync()
}
