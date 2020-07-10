package com.itv.bucky

import com.itv.bucky.consume.DeliveryMode
import com.itv.bucky.publish.{ContentEncoding, ContentType, MessageProperties}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope}

object MessagePropertiesConverters {

  import scala.jdk.CollectionConverters._

  def apply(envelope: RabbitMQEnvelope): Envelope =
    Envelope(envelope.getDeliveryTag, envelope.isRedeliver, ExchangeName(envelope.getExchange), RoutingKey(envelope.getRoutingKey))

  def apply(properties: BasicProperties): MessageProperties =
    MessageProperties(
      contentType = Option(properties.getContentType).map(ContentType.apply),
      contentEncoding = Option(properties.getContentEncoding).map(ContentEncoding.apply),
      headers = Option(properties.getHeaders).map(_.asScala).fold[Map[String, AnyRef]](Map())(_.toMap),
      deliveryMode = Option(properties.getDeliveryMode).map(value => DeliveryMode(value)),
      priority = priorityOf(properties),
      correlationId = Option(properties.getCorrelationId),
      replyTo = Option(properties.getReplyTo),
      expiration = Option(properties.getExpiration),
      messageId = Option(properties.getMessageId),
      timestamp = Option(properties.getTimestamp),
      messageType = Option(properties.getType),
      userId = Option(properties.getUserId),
      appId = Option(properties.getAppId),
      clusterId = Option(properties.getClusterId)
    )

  def apply(properties: MessageProperties): BasicProperties =
    new BasicProperties.Builder()
      .contentType(toJString(properties.contentType.map(_.value)))
      .contentEncoding(toJString(properties.contentEncoding.map(_.value)))
      .headers(if (properties.headers.isEmpty) null else properties.headers.asJava)
      .deliveryMode(toJInt(properties.deliveryMode.map(_.value)))
      .priority(toJInt(properties.priority))
      .correlationId(toJString(properties.correlationId))
      .replyTo(toJString(properties.replyTo))
      .expiration(toJString(properties.expiration))
      .messageId(toJString(properties.messageId))
      .timestamp(toJDate(properties.timestamp))
      .`type`(toJString(properties.messageType))
      .userId(toJString(properties.userId))
      .appId(toJString(properties.appId))
      .clusterId(toJString(properties.clusterId))
      .build()

  import java.util.Date

  private def toJInt(value: Option[Int]): Integer                  = value.fold[Integer](null)(value => value)
  private def toJString(value: Option[String]): String             = value.fold[String](null)(identity)
  private def toJDate(value: Option[Date]): Date                   = value.fold[Date](null)(identity)
  private def priorityOf(properties: BasicProperties): Option[Int] = Option[Integer](properties.getPriority).map(_.toInt)
}
