package com.itv.bucky.publish

import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import cats.effect.concurrent.{Deferred, Ref}
import com.rabbitmq.client.ConfirmListener
import com.typesafe.scalalogging.StrictLogging
import scala.language.higherKinds

import scala.collection.immutable.TreeMap

sealed trait ConfirmationResult
case object ConfirmationAck extends ConfirmationResult
case object ConfirmationNack extends ConfirmationResult
case class ConfirmationAborted(exception: Throwable) extends ConfirmationResult

private[bucky] case class PendingConfirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, ConfirmationResult]]])(
    implicit F: ConcurrentEffect[F])
    extends ConfirmListener
    with StrictLogging {

  def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, ConfirmationResult]]] =
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
      _          <- toComplete.traverse(_.complete(ConfirmationAck))
    } yield ()).toIO
      .unsafeRunSync()

  def abortAll(exception: Throwable): F[Unit] =
    for {
      current <- pendingConfirmations.get
      _ <- F.delay(logger.warn(s"Aborting ${current.size} pending confirmations due to $exception"))
      _ <- (
        current.valuesIterator.toList.traverse { deferred =>
          deferred.complete(ConfirmationAborted(exception))
        }
      )
      _ <- pendingConfirmations.set(current.empty)
    } yield ()

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    (for {
      toComplete <- pop(deliveryTag, multiple)
      _          <- F.delay(logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
      _          <- toComplete.traverse(_.complete(ConfirmationNack))
    } yield ()).toIO
      .unsafeRunSync()
}
