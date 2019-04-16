package com.itv.bucky.pattern.requeue

import cats.effect.Sync
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try
import cats.implicits._
import cats.effect.implicits._
import com.itv.bucky.{ExchangeName, Handler, Publisher, RequeueHandler}
import com.itv.bucky.consume.{Ack, ConsumeAction, DeadLetter, Delivery, PublishCommand, RequeueConsumeAction}

case class RequeueTransformer[F[_]](
    requeuePublisher: Publisher[F, PublishCommand],
    requeueExchange: ExchangeName,
    requeuePolicy: RequeuePolicy,
    onFailure: RequeueConsumeAction,
    onFailureAction: Delivery => F[Unit]
)(handler: RequeueHandler[F, Delivery])(implicit F: Sync[F])
    extends Handler[F, Delivery]
    with StrictLogging {

  private val requeueCountHeaderName = "x-bucky-requeue-counter"

  private def remainingAttempts(delivery: Delivery): Option[Int] =
    for {
      requeueCountAnyRef <- delivery.properties.headers.get(requeueCountHeaderName)
      requeueCount       <- Try(requeueCountAnyRef.toString.toInt).toOption
    } yield requeueCount

  private def buildRequeuePublishCommand(delivery: Delivery, remainingAttempts: Int, requeueAfter: FiniteDuration): PublishCommand = {
    val properties = delivery.properties
      .withHeader(requeueCountHeaderName -> remainingAttempts.toString)
      .copy(expiration = Some(requeueAfter.toMillis.toString))

    PublishCommand(requeueExchange, delivery.envelope.routingKey, properties, delivery.body)
  }

  override def apply(delivery: Delivery): F[ConsumeAction] = {
    def perform(action: RequeueConsumeAction): F[ConsumeAction] =
      action match {
        case com.itv.bucky.consume.Requeue =>
          remainingAttempts(delivery) match {
            case Some(value) if value < 1 =>
              onFailureAction(delivery).map(_ => DeadLetter)
            case Some(value) =>
              requeuePublisher(buildRequeuePublishCommand(delivery, value - 1, requeuePolicy.requeueAfter)).map(_ => Ack)
            case None =>
              if (requeuePolicy.maximumProcessAttempts <= 1)
                onFailureAction(delivery).map(_ => DeadLetter)
              else {
                val initialRemainingAttempts = requeuePolicy.maximumProcessAttempts - 2
                requeuePublisher(buildRequeuePublishCommand(delivery, initialRemainingAttempts, requeuePolicy.requeueAfter)).map(_ => Ack)
              }
          }
        case other: ConsumeAction => F.point(other)
      }

    val safePerform = F.flatMap(F.delay(handler(delivery)))(identity)
    F.handleErrorWith(F.flatMap(safePerform)(perform)) {
      case t: Throwable =>
        logger.error(s"Unable to process ${delivery.body} due to handler failure, will $onFailure", t)
        perform(onFailure)
    }
  }

}
