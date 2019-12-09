package com.itv.bucky

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.circe.auto._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class CircePayloadUnmarshallersTest extends FunSuite {

  case class Entity(foo: String)

  test("it should parse a json object") {
    val expectedValue    = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = implicitly[PayloadUnmarshaller[Json]].unmarshal(payload).right.get

    jsonResult.hcursor.get[String]("foo") shouldBe Right(expectedValue)
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Json]].unmarshal(payload) shouldBe 'left
  }

  test(s"it should convert to a type") {
    val expectedValue = s"Random-${new Random().nextInt(100)}"
    val payload       = validJson(expectedValue)

    val someResult = implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload).right.get
    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload) shouldBe 'left
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result      = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).right.get.noSpaces
    val expected    = StringPayloadUnmarshaller.unmarshal(jsonPayload).right.get
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result   = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Entity]).right.get.asJson.noSpaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).right.get

    result shouldBe expected
  }

  def validJson(value: String): Payload = Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)

  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
