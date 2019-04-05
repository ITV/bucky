package com.itv

import java.nio.charset.{Charset, StandardCharsets}

import cats.effect.ConcurrentEffect
import com.itv.bucky.consume.{ConsumeAction, Delivery, RequeueConsumeAction}
import com.itv.bucky.publish.PublishCommandBuilder

import scala.language.higherKinds

package object bucky {

  type Publisher[F[_], -T]      = T => F[Unit]
  type Handler[F[_], -T]        = T => F[ConsumeAction]
  type RequeueHandler[F[_], -T] = T => F[RequeueConsumeAction]
  type Bindings                 = PartialFunction[RoutingKey, QueueName]
  type PayloadUnmarshaller[T]   = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T]  = Unmarshaller[Delivery, T]
  type UnmarshalResult[T]       = Either[Throwable, T]

  case class RoutingKey(value: String)
  case class ExchangeName(value: String)
  case class QueueName(value: String)

  implicit def consumerSyntax[F[_]]: AmqpClient[F] => consume.ConsumerSugar[F] =
    com.itv.bucky.consume.ConsumerSugar[F]
  implicit def publisherSyntax[F[_]]: AmqpClient[F] => publish.PublisherSugar[F] =
    com.itv.bucky.publish.PublisherSugar[F]
  def publishCommandBuilder[T](marshaller: PayloadMarshaller[T]): PublishCommandBuilder.NothingSet[T] =
    PublishCommandBuilder.publishCommandBuilder[T](marshaller)

  implicit class LoggingSyntax[F[_]](client: AmqpClient[F])(implicit F: ConcurrentEffect[F]) {
    def withLogging(charset: Charset = StandardCharsets.UTF_8): AmqpClient[F] = LoggingAmqpClient(client, charset)
  }

}
