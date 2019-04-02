package com.itv.bucky

import java.util.Date

import com.itv.bucky.consume.DeliveryMode

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
}
