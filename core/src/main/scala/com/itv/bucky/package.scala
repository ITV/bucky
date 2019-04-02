package com.itv

import com.itv.bucky.consume.{ConsumeAction, Delivery, RequeueConsumeAction}

import scala.language.higherKinds

package object bucky {

  type Publisher[F[_], -T]      = T => F[Unit]
  type Handler[F[_], -T]        = T => F[ConsumeAction]
  type RequeueHandler[F[_], -T] = T => F[RequeueConsumeAction]
  type Bindings                 = PartialFunction[RoutingKey, QueueName]
  type PayloadUnmarshaller[T]   = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T]  = Unmarshaller[Delivery, T]

  case class RoutingKey(value: String)
  case class ExchangeName(value: String)
  case class QueueName(value: String)
}
