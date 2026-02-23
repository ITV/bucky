package fs2rabbit

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.backend.fs2rabbit.Fs2RabbitAmqpClient
import com.itv.bucky.consume.DeliveryMode
import com.itv.bucky.publish.{MessageProperties, PublishCommandBuilder}
import com.itv.bucky.{AmqpClientConfig, ExchangeName, QueueName, RoutingKey, consume, publish}
import com.rabbitmq.client.impl.LongStringHelper
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpProperties, DeliveryTag, HeaderKey, Headers, ShortString}
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scodec.bits.ByteVector

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import scala.jdk.CollectionConverters._

class Fs2RabbitAmqpClientSpec extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues {

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("localhost", 5672, "guest", "guest")

  val now: Instant = Instant.now().truncatedTo(ChronoUnit.SECONDS)

  val amqpMessageHeaders: Headers = Headers(Map(
    HeaderKey("bigDecimal") -> model.AmqpFieldValue.DecimalVal.unsafeFrom(1),
    HeaderKey("instant")    -> model.AmqpFieldValue.TimestampVal.from(now),
    HeaderKey("date")       -> model.AmqpFieldValue.TimestampVal.from(Date.from(now)),
    HeaderKey("map") -> model.AmqpFieldValue.TableVal(
      Map(
        ShortString.unsafeFrom("inside") -> model.AmqpFieldValue.IntVal(1)
      )
    ),
    HeaderKey("byte")       -> model.AmqpFieldValue.ByteVal('c'.toByte),
    HeaderKey("double")     -> model.AmqpFieldValue.DoubleVal(Double.box(4.5)),
    HeaderKey("float")      -> model.AmqpFieldValue.FloatVal(Double.box(4.5).toFloat),
    HeaderKey("short")      -> model.AmqpFieldValue.ShortVal(Short.box(1)),
    HeaderKey("byteArray")  -> model.AmqpFieldValue.ByteArrayVal(ByteVector(0.toByte, 1.toByte)),
    HeaderKey("int")        -> model.AmqpFieldValue.IntVal(Integer.MIN_VALUE),
    HeaderKey("long")       -> model.AmqpFieldValue.LongVal(Long.MinValue),
    HeaderKey("string")     -> model.AmqpFieldValue.StringVal("blah"),
    HeaderKey("longString") -> model.AmqpFieldValue.StringVal("blahlong"),
    HeaderKey("list") -> model.AmqpFieldValue.ArrayVal(
      Vector(
        model.AmqpFieldValue.IntVal(1),
        model.AmqpFieldValue.IntVal(2),
        model.AmqpFieldValue.IntVal(3)
      )
    )
  ))

  val messagePropertyHeaders: Map[String, AnyRef] = Map(
    "bigDecimal" -> java.math.BigDecimal.ONE,
    "instant"    -> now,
    "date"       -> Date.from(now),
    "map" -> Map(
      "inside" -> 1
    ).asJava,
    "byte"       -> Byte.box('c'.toByte),
    "double"     -> Double.box(4.5),
    "float"      -> Float.box(Double.box(4.5).toFloat),
    "short"      -> Short.box(1),
    "byteArray"  -> Array(0.toByte, 1.toByte),
    "int"        -> Int.box(Integer.MIN_VALUE),
    "long"       -> Long.box(Long.MinValue),
    "string"     -> "blah",
    "longString" -> LongStringHelper.asLongString("blahlong"),
    "list"       -> List(1, 2, 3).asJava
  )

  "deliveryEncoder" should {
    val basicPublishCommand = PublishCommandBuilder
      .publishCommandBuilder(StringPayloadMarshaller)
      .using(ExchangeName("blah"))
      .using(RoutingKey("routing"))
      .toPublishCommand("message")

    "decode a basic publish command" in {
      Fs2RabbitAmqpClient.deliveryEncoder[IO].apply(basicPublishCommand).asserting { amqpMessage =>
        amqpMessage.payload shouldBe "message".getBytes
      }

    }

    "decode a publish command with all the properties, apart from headers" in {
      val publishCommand = basicPublishCommand.copy(basicProperties =
        MessageProperties(
          contentType = Some(publish.ContentType.textPlain),
          contentEncoding = Some(publish.ContentEncoding.utf8),
          headers = Map.empty,
          deliveryMode = Some(consume.DeliveryMode.persistent),
          priority = Some(1),
          correlationId = Some("correlationid"),
          replyTo = Some("replyto"),
          expiration = Some("expiration"),
          messageId = Some("messageId"),
          timestamp = Some(Date.from(Instant.now())),
          messageType = Some("messageType"),
          userId = Some("userId"),
          appId = Some("appId"),
          clusterId = Some("clusterId")
        )
      )

      Fs2RabbitAmqpClient.deliveryEncoder[IO].apply(publishCommand).asserting { amqpMessage =>
        amqpMessage.properties shouldBe AmqpProperties(
          contentType = Some("text/plain"),
          contentEncoding = Some("utf-8"),
          priority = Some(1),
          deliveryMode = Some(model.DeliveryMode.Persistent),
          correlationId = Some("correlationid"),
          messageId = Some("messageId"),
          `type` = Some("messageType"),
          userId = Some("userId"),
          appId = Some("appId"),
          expiration = Some("expiration"),
          replyTo = Some("replyto"),
          clusterId = Some("clusterId"),
          timestamp = Some(publishCommand.basicProperties.timestamp.get.toInstant),
          headers = Headers.empty
        )

      }
    }

    "decode a publish command with headers" in {
      val publishCommand = basicPublishCommand.copy(basicProperties =
        MessageProperties.basic.copy(
          headers = messagePropertyHeaders
        )
      )

      Fs2RabbitAmqpClient.deliveryEncoder[IO].apply(publishCommand).asserting { amqpMessage =>
        amqpMessage.properties.headers shouldBe amqpMessageHeaders
      }

    }
  }

  "deliveryDecoder" should {
    val basicEnvelope = AmqpEnvelope(
      DeliveryTag(123L),
      "payload".getBytes,
      AmqpProperties.empty,
      model.ExchangeName("exchange"),
      model.RoutingKey("routing"),
      redelivered = false
    )
    val queueName = QueueName("queue")

    "decode a basic AmqpEnvelope" in {
      Fs2RabbitAmqpClient.deliveryDecoder[IO](queueName).apply(basicEnvelope).map { delivery =>
        delivery.envelope.deliveryTag shouldBe 123L
        delivery.body.value shouldBe "payload".getBytes
        delivery.envelope.exchangeName.value shouldBe "exchange"
        delivery.envelope.routingKey.value shouldBe "routing"
        delivery.envelope.redeliver shouldBe false
      }

    }

    "decode an AmqpEnvelope with properties" in {

      val instant = Instant.ofEpochSecond(now.getEpochSecond)

      val amqpProperties = AmqpProperties.apply(
        contentType = Some("content-type"),
        contentEncoding = Some("content-encoding"),
        priority = Some(5),
        deliveryMode = Some(model.DeliveryMode.Persistent),
        correlationId = Some("correlation-id"),
        messageId = Some("message-id"),
        `type` = Some("type"),
        userId = Some("user"),
        appId = Some("app"),
        expiration = Some("expiry"),
        replyTo = Some("reply"),
        clusterId = Some("cluster"),
        timestamp = Some(instant),
        headers = amqpMessageHeaders
      )

      Fs2RabbitAmqpClient.deliveryDecoder[IO](queueName).apply(basicEnvelope.copy(properties = amqpProperties)).asserting { delivery =>
        delivery.properties.headers.get("bigDecimal").map(_.toString) shouldBe Some(java.math.BigDecimal.ONE.toString)
        delivery.properties.headers.get("instant").value.asInstanceOf[Date].toInstant shouldBe now
        delivery.properties.headers.get("date") shouldBe Some(Date.from(now))
        delivery.properties.headers.get("map") shouldBe Some(
          Map(
            "inside" -> 1
          ).asJava
        )
        delivery.properties.headers.get("byte") shouldBe Some(Byte.box('c'.toByte))
        delivery.properties.headers.get("double") shouldBe Some(Double.box(4.5))
        delivery.properties.headers.get("float") shouldBe Some(Float.box(Double.box(4.5).toFloat))
        delivery.properties.headers.get("short") shouldBe Some(Short.box(1))
        delivery.properties.headers.get("byteArray").value.asInstanceOf[Array[Byte]] shouldBe Array(0.toByte, 1.toByte)
        delivery.properties.headers.get("int") shouldBe Some(Int.box(Integer.MIN_VALUE))
        delivery.properties.headers.get("long") shouldBe Some(Long.box(Long.MinValue))
        delivery.properties.headers.get("string") shouldBe Some("blah")
        delivery.properties.headers.get("longString") shouldBe Some("blahlong")
        delivery.properties.headers.get("list") shouldBe Some(List(1, 2, 3).asJava)

        delivery.properties.contentType.map(_.value) shouldBe amqpProperties.contentType
        delivery.properties.contentEncoding.map(_.value) shouldBe amqpProperties.contentEncoding
        delivery.properties.deliveryMode shouldBe Some(DeliveryMode.persistent)
        delivery.properties.priority shouldBe Some(5)
        delivery.properties.correlationId shouldBe Some("correlation-id")
        delivery.properties.replyTo shouldBe Some("reply")
        delivery.properties.expiration shouldBe Some("expiry")
        delivery.properties.messageId shouldBe Some("message-id")
        delivery.properties.timestamp shouldBe Some(Date.from(instant))
        delivery.properties.messageType shouldBe Some("type")
        delivery.properties.userId shouldBe Some("user")
        delivery.properties.appId shouldBe Some("app")
        delivery.properties.clusterId shouldBe Some("cluster")
      }
    }

  }

}
