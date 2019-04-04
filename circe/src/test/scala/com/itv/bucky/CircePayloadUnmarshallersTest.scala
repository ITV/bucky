package com.itv.bucky

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import PayloadMarshaller.StringPayloadMarshaller
import Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.UnmarshalResult.Failure
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class CircePayloadUnmarshallersTest extends FunSuite {

  import com.itv.bucky.test._
  import com.itv.bucky.circe._

  case class Entity(foo: String)

  test("it should parse a json object") {
    val expectedValue    = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = implicitly[PayloadUnmarshaller[Json]].unmarshal(payload).getSuccess

    jsonResult.hcursor.get[String]("foo") shouldBe Right(expectedValue)
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Json]].unmarshal(payload) shouldBe a[Failure]
  }

  test(s"it should convert to a type") {
    val expectedValue = s"Random-${new Random().nextInt(100)}"
    val payload       = validJson(expectedValue)

    val someResult = implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload).getSuccess
    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload    = invalidJson()
    implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload) shouldBe a[Failure]
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result      = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).getSuccess.noSpaces
    val expected    = StringPayloadUnmarshaller.unmarshal(jsonPayload).getSuccess
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result   = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Entity]).getSuccess.asJson.noSpaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).getSuccess

    result shouldBe expected
  }

  def validJson(value: String): Payload = Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)

  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
