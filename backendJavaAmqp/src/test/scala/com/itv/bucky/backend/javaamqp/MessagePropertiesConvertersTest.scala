package com.itv.bucky.backend.javaamqp

import com.itv.bucky.consume.DeliveryMode
import com.itv.bucky.publish.{ContentEncoding, ContentType, MessageProperties}
import com.rabbitmq.client.AMQP.BasicProperties
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.Date
import scala.jdk.CollectionConverters._

class MessagePropertiesConvertersTest extends AnyFunSuite {

  test("should be able to convert minimal basic properties to message properties") {

    val basicProperties: BasicProperties             = new BasicProperties()
    val messageProperties: MessageProperties = MessagePropertiesConverters.apply(basicProperties)

    messageProperties shouldBe MessageProperties(
      contentType = None,
      contentEncoding = None,
      headers = Map.empty,
      deliveryMode = None,
      priority = None,
      correlationId = None,
      replyTo = None,
      expiration = None,
      messageId = None,
      timestamp = None,
      messageType = None,
      userId = None,
      appId = None,
      clusterId = None
    )

  }

  test("should be able to convert full basic properties to message properties") {

    val date    = new Date()
    val headers = Map[String, AnyRef]("h1" -> "v1", "h2" -> "XXX")

    val fullBasicProperties = new BasicProperties(
      "application/xml",
      "UTF-8",
      headers.asJava,
      1,
      2,
      "correlationId",
      "replyTo",
      "10s",
      "ABC",
      date,
      "type",
      "userId",
      "appId",
      "clusterId"
    )

    MessagePropertiesConverters(fullBasicProperties) shouldBe MessageProperties(
      contentType = Some(ContentType("application/xml")),
      contentEncoding = Some(ContentEncoding("UTF-8")),
      headers = headers,
      deliveryMode = Some(DeliveryMode.nonPersistent),
      priority = Some(2),
      correlationId = Some("correlationId"),
      replyTo = Some("replyTo"),
      expiration = Some("10s"),
      messageId = Some("ABC"),
      timestamp = Some(date),
      messageType = Some("type"),
      userId = Some("userId"),
      appId = Some("appId"),
      clusterId = Some("clusterId")
    )

  }

}
