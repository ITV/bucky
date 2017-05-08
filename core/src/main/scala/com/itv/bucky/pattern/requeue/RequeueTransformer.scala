package com.itv.bucky.pattern.requeue

import com.typesafe.scalalogging.StrictLogging
import com.itv.bucky.{Ack, ConsumeAction, DeadLetter, _}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try

case class RequeueTransformer[F[_], E](requeuePublisher: Publisher[F, PublishCommand],
                                    requeueExchange: ExchangeName,
                                    requeuePolicy: RequeuePolicy,
                                    onFailure: RequeueConsumeAction)(handler: RequeueHandler[F, Delivery])
                                   (implicit F: MonadError[F, E]) extends Handler[F, Delivery] with StrictLogging {

  private val requeueCountHeaderName = "x-bucky-requeue-counter"

  private def remainingAttempts(delivery: Delivery): Option[Int] = for {
    requeueCountAnyRef <- delivery.properties.headers.get(requeueCountHeaderName)
    requeueCount <- Try(requeueCountAnyRef.toString.toInt).toOption
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
        case com.itv.bucky.Requeue => remainingAttempts(delivery) match {
          case Some(value) if value < 1 => F.apply(DeadLetter)
          case Some(value) => F.map(requeuePublisher(buildRequeuePublishCommand(delivery, value - 1, requeuePolicy.requeueAfter)))(_ => Ack)
          case None =>
            if (requeuePolicy.maximumProcessAttempts <= 1)
              F.apply(DeadLetter)
            else {
              val initialRemainingAttempts = requeuePolicy.maximumProcessAttempts - 2
              F.map(requeuePublisher(buildRequeuePublishCommand(delivery, initialRemainingAttempts, requeuePolicy.requeueAfter)))(_ => Ack)
            }
        }
        case other: ConsumeAction => F.apply(other)
      }


    val safePerform = F.flatMap(F.apply(handler(delivery)))(identity)
    F.handleError(F.flatMap(safePerform)(perform)){ case t: Throwable =>
        logger.error(s"Unable to process ${delivery.body} due to handler failure, will $onFailure", t)
        perform(onFailure)
      }
  }

}
