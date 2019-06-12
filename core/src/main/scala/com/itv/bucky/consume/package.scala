package com.itv.bucky

import java.lang.management.ManagementFactory

import cats.ApplicativeError
import cats.effect.{Resource, Sync}
import com.itv.bucky.Unmarshaller.toDeliveryUnmarshaller
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.{RequeueOps, RequeuePolicy}
import com.itv.bucky.publish.MessageProperties

import scala.concurrent.duration._
import scala.language.higherKinds

package object consume {
  sealed trait ConsumeAction
  sealed trait RequeueConsumeAction
  case object Ack                extends ConsumeAction with RequeueConsumeAction
  case object DeadLetter         extends ConsumeAction with RequeueConsumeAction
  case object RequeueImmediately extends ConsumeAction with RequeueConsumeAction
  case object Requeue            extends RequeueConsumeAction

  object Handler {
    def apply[F[_], T](f: T => F[ConsumeAction]): Handler[F, T] = (message: T) => f(message)
  }

  object RequeueHandler {
    def apply[F[_], T](f: T => F[RequeueConsumeAction]): RequeueHandler[F, T] = (message: T) => f(message)
  }
  case class Delivery(body: Payload, consumerTag: ConsumerTag, envelope: Envelope, properties: MessageProperties)
  case class DeliveryMode(value: Int)
  object DeliveryMode {
    val persistent    = DeliveryMode(2)
    val nonPersistent = DeliveryMode(1)
  }

  case class ConsumerTag(value: String)
  object ConsumerTag {
    def create(queueName: QueueName): ConsumerTag =
      ConsumerTag(s"${ManagementFactory.getRuntimeMXBean.getName}-${queueName.value}")
  }
  case class PublishCommand(exchange: ExchangeName, routingKey: RoutingKey, basicProperties: MessageProperties, body: Payload) {
    def description = s"${exchange.value}:${routingKey.value} $body"
  }

  implicit class ConsumerSugar[F[_]](amqpClient: AmqpClient[F]) {

    def registerConsumerOf[T](queueName: QueueName, handler: Handler[F, T],exceptionalAction: ConsumeAction = DeadLetter, prefetchCount : Int = defaultPreFetchCount )(implicit payloadUnmarshaller: PayloadUnmarshaller[T], ae: ApplicativeError[F, Throwable]): Resource[F, Unit] = {
      amqpClient.registerConsumer(queueName, (delivery: Delivery) => {
        payloadUnmarshaller.unmarshal(delivery.body) match {
          case Right(value) =>
            handler.apply(value)
          case Left(e) =>
            ae.raiseError(e)
        }
      }, exceptionalAction, prefetchCount)
    }

    def registerRequeueConsumerOf[T](
                                      queueName: QueueName,
                                      handler: RequeueHandler[F, T],
                                      requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
                                      onFailure: RequeueConsumeAction = Requeue,
                                      unmarshalFailureAction: RequeueConsumeAction = DeadLetter)(implicit unmarshaller: PayloadUnmarshaller[T], F: Sync[F]): Resource[F, Unit] =
      new RequeueOps(amqpClient).requeueDeliveryHandlerOf(
        queueName,
        handler,
        requeuePolicy,
        toDeliveryUnmarshaller(unmarshaller),
        onFailure,
        unmarshalFailureAction = unmarshalFailureAction
      )

    def registerRequeueConsumer(
                                queueName: QueueName,
                                handler: RequeueHandler[F, Delivery],
                                requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
                                onFailure: RequeueConsumeAction = Requeue,
                                prefetchCount: Int = defaultPreFetchCount
    )(implicit F: Sync[F]): Resource[F, Unit] =
      new RequeueOps(amqpClient).requeueOf(queueName, handler, requeuePolicy, onFailure, prefetchCount= prefetchCount)


  }
}
