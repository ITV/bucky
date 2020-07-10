package com.itv.bucky

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.circe.auto._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.{EitherValues, FunSuite, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.exceptions.TestFailedException

import scala.util.Random

class CircePayloadUnmarshallersTest extends AnyFunSuite with EitherValues with OptionValues {

  case class Entity(foo: String)

  test("it should parse a json object") {
    val expectedValue    = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = implicitly[PayloadUnmarshaller[Json]].unmarshal(payload).toOption.value

    jsonResult.hcursor.get[String]("foo") shouldBe Right(expectedValue)
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Json]].unmarshal(payload).left.value shouldBe a[Throwable]
  }

  test(s"it should convert to a type") {
    val expectedValue = s"Random-${new Random().nextInt(100)}"
    val payload       = validJson(expectedValue)

    val someResult = implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload).toOption.value
    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload).left.value shouldBe a[Throwable]
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result      = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).toOption.value.noSpaces
    val expected    = StringPayloadUnmarshaller.unmarshal(jsonPayload).toOption.value
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result   = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Entity]).toOption.value.asJson.noSpaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).toOption.value

    result shouldBe expected
  }

  def validJson(value: String): Payload = Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)

  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
