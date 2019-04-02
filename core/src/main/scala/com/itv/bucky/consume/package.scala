package com.itv.bucky

import java.lang.management.ManagementFactory

import com.itv.bucky.publish.MessageProperties
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
}
