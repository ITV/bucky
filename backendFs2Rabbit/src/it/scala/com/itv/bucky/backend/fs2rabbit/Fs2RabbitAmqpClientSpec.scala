package com.itv.bucky.backend.fs2rabbit

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.publish.{MessageProperties, PublishCommandBuilder}
import com.itv.bucky.{AmqpClientConfig, ExchangeName, RoutingKey, consume, publish}
import com.rabbitmq.client.impl.LongStringHelper
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AmqpProperties, ShortString}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scodec.bits.ByteVector

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters._

class Fs2RabbitAmqpClientSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("localhost", 5672, "guest", "guest")

  "deliveryEncoder" should {
    val basicPublishCommand = PublishCommandBuilder
      .publishCommandBuilder(StringPayloadMarshaller)
      .using(ExchangeName("blah"))
      .using(RoutingKey("routing"))
      .toPublishCommand("message")

    "decode a basic publish command" in {
      Fs2RabbitAmqpClient[IO](amqpClientConfig).use { amqpClient =>
        amqpClient.deliveryEncoder(basicPublishCommand).map { amqpMessage =>
          amqpMessage.payload shouldBe "message".getBytes
        }
      }
    }

    "decode a publish command with all the properties, apart from headers" in {
      Fs2RabbitAmqpClient[IO](amqpClientConfig).use { amqpClient =>
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

        amqpClient.deliveryEncoder(publishCommand).map { amqpMessage =>
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
            headers = Map.empty
          )
        }
      }
    }

    "decode a publish command with headers" in {
      Fs2RabbitAmqpClient[IO](amqpClientConfig).use { amqpClient =>
        val publishCommand = basicPublishCommand.copy(basicProperties =
          MessageProperties.basic.copy(
            headers = Map(
              "bigDecimal" -> java.math.BigDecimal.ONE,
              "instant"    -> Instant.now(),
              "date"       -> Date.from(Instant.now()),
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
          )
        )

        amqpClient.deliveryEncoder(publishCommand).map { amqpMessage =>
          amqpMessage.properties.headers shouldBe Map(
            "bigDecimal" -> model.AmqpFieldValue.DecimalVal.unsafeFrom(1),
            "instant"    -> model.AmqpFieldValue.TimestampVal.from(Instant.now()),
            "date"       -> model.AmqpFieldValue.TimestampVal.from(Date.from(Instant.now())),
            "map" -> model.AmqpFieldValue.TableVal(
              Map(
                ShortString.unsafeFrom("inside") -> model.AmqpFieldValue.IntVal(1)
              )
            ),
            "byte"       -> model.AmqpFieldValue.ByteVal('c'.toByte),
            "double"     -> model.AmqpFieldValue.DoubleVal(Double.box(4.5)),
            "float"      -> model.AmqpFieldValue.FloatVal(Double.box(4.5).toFloat),
            "short"      -> model.AmqpFieldValue.ShortVal(Short.box(1)),
            "byteArray"  -> model.AmqpFieldValue.ByteArrayVal(ByteVector(0.toByte, 1.toByte)),
            "int"        -> model.AmqpFieldValue.IntVal(Integer.MIN_VALUE),
            "long"       -> model.AmqpFieldValue.LongVal(Long.MinValue),
            "string"     -> model.AmqpFieldValue.StringVal("blah"),
            "longString" -> model.AmqpFieldValue.StringVal("blahlong"),
            "list" -> model.AmqpFieldValue.ArrayVal(
              Vector(
                model.AmqpFieldValue.IntVal(1),
                model.AmqpFieldValue.IntVal(2),
                model.AmqpFieldValue.IntVal(3)
              )
            )
          )
        }
      }
    }
  }

}
