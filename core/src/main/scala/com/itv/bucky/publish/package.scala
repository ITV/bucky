package com.itv.bucky

import java.util.Date

import cats.effect.Sync
import com.itv.bucky.consume.{DeliveryMode, PublishCommand}

import scala.language.higherKinds

package object publish {
  case class ContentType(value: String)
  object ContentType {
    val octetStream = ContentType("application/octet-stream")
    val textPlain   = ContentType("text/plain")
  }

  case class ContentEncoding(value: String)
  object ContentEncoding {
    val utf8 = ContentEncoding("utf-8")
  }

  case class MessageProperties(
      contentType: Option[ContentType],
      contentEncoding: Option[ContentEncoding],
      headers: Map[String, AnyRef],
      deliveryMode: Option[DeliveryMode],
      priority: Option[Int],
      correlationId: Option[String],
      replyTo: Option[String],
      expiration: Option[String],
      messageId: Option[String],
      timestamp: Option[Date],
      messageType: Option[String],
      userId: Option[String],
      appId: Option[String],
      clusterId: Option[String]
  ) {
    def withHeader(header: (String, AnyRef)): MessageProperties = this.copy(
      headers = this.headers + header
    )
  }

  object MessageProperties {
    val minimalBasic                              = MessageProperties(None, None, Map(), None, None, None, None, None, None, None, None, None, None, None)
    val minimalPersistentBasic: MessageProperties = minimalBasic.copy(deliveryMode = Some(DeliveryMode.persistent))

    val basic: MessageProperties =
      minimalBasic.copy(contentType = Some(ContentType.octetStream), deliveryMode = Some(DeliveryMode.nonPersistent), priority = Some(0))

    val persistentBasic: MessageProperties =
      minimalBasic.copy(contentType = Some(ContentType.octetStream), deliveryMode = Some(DeliveryMode.persistent), priority = Some(0))

    val textPlain: MessageProperties = minimalBasic.copy(
      contentType = Some(ContentType.textPlain),
      deliveryMode = Some(DeliveryMode.nonPersistent),
      priority = Some(0)
    )

    val persistentTextPlain: MessageProperties = minimalBasic.copy(
      contentType = Some(ContentType.textPlain),
      deliveryMode = Some(DeliveryMode.persistent),
      priority = Some(0)
    )
  }

  implicit class PublisherSugar[F[_]](amqpClient: AmqpClient[F]) {

    def publisherOf[T](implicit publishCommandBuilder: PublishCommandBuilder[T]): Publisher[F, T] = {
      val basePublisher = amqpClient.publisher()
      value: T => {
        val command = publishCommandBuilder.toPublishCommand(value)
        basePublisher.apply(command)
      }
    }

    def publisherOf[T](exchangeName: ExchangeName, routingKey: RoutingKey)(implicit marshaller: PayloadMarshaller[T]): Publisher[F, T] = {
      val pcb =
        PublishCommandBuilder.publishCommandBuilder(marshaller)
            .using(exchangeName)
            .using(routingKey)
      publisherOf[T](pcb)
    }

    def publisherWithHeadersOf[T](exchangeName: ExchangeName, routingKey: RoutingKey)(implicit F: Sync[F], marshaller: PayloadMarshaller[T]): PublisherWithHeaders[F, T] ={
      val pcb =
        PublishCommandBuilder.publishCommandBuilder(marshaller)
        .using(exchangeName)
        .using(routingKey)
      publisherWithHeadersOf[T](pcb)
    }

    def publisherWithHeadersOf[T](commandBuilder: PublishCommandBuilder[T])(implicit F: Sync[F]): PublisherWithHeaders[F, T] =
      (message: T, headers: Map[String, AnyRef]) =>
        F.flatMap(F.delay {
          val command = commandBuilder.toPublishCommand(message)

          command.copy(basicProperties = headers.foldLeft(command.basicProperties) {
            case (props, (headerName, headerValue)) => props.withHeader(headerName -> headerValue)
          })
        })(amqpClient.publisher())

  }
}
