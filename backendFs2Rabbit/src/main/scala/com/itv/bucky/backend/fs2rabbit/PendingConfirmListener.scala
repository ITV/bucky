package com.itv.bucky.backend.fs2rabbit

import cats.effect._
import cats.effect.std.Dispatcher
import cats.implicits._
import com.rabbitmq.client.{AMQP, ConfirmListener, ReturnListener}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap

private[bucky] case class PendingConfirmListener[F[_]](
    pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Option[Throwable]]]],
    pendingReturn: Ref[F, Option[Throwable]],
    dispatcher: Dispatcher[F]
)(implicit F: Sync[F])
    extends ConfirmListener
    with ReturnListener
    with StrictLogging {

  def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, Option[Throwable]]]] =
    pendingConfirmations.modify { x =>
      if (multiple) {
        val entries = x.rangeUntil(deliveryTag + 1).toList
        (x -- entries.map { case (key, _) => key }, entries.map { case (_, value) => value })
      } else {
        (x - deliveryTag, x.get(deliveryTag).toList)
      }
    }

  override def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
    dispatcher.unsafeRunSync(
      for {
        returnRef  <- pendingReturn.getAndSet(None)
        toComplete <- pop(deliveryTag, multiple)
        _          <- F.delay(logger.info("Received ack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
        _          <- toComplete.traverse(_.complete(returnRef))
      } yield ()
    )

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    dispatcher.unsafeRunSync(
      for {
        toComplete <- pop(deliveryTag, multiple)
        _          <- F.delay(logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple))
        _          <- pendingReturn.set(None)
        _          <- toComplete.traverse(_.complete(Some(new Throwable(s"Received Nack for delivery tag: $deliveryTag and multiple: $multiple"))))
      } yield ()
    )

  override def handleReturn(
      replyCode: Int,
      replyText: String,
      exchange: String,
      routingKey: String,
      properties: AMQP.BasicProperties,
      body: Array[Byte]
  ): Unit =
    dispatcher.unsafeRunSync(pendingReturn.set(Some(new Throwable(s"Message publish returned with $replyCode: " + replyText))))
}
