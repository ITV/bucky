package com.itv.bucky

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.circe.auto._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.exceptions.TestFailedException

import scala.util.Random

class CircePayloadUnmarshallersTest extends AnyFunSuite {

  case class Entity(foo: String)

  test("it should parse a json object") {
    val expectedValue    = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Option[Json] = implicitly[PayloadUnmarshaller[Json]].unmarshal(payload).toOption

    jsonResult.map(_.hcursor.get[String]("foo")) shouldBe Some(Right(expectedValue))
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Json]].unmarshal(payload) shouldBe a[Left[_,_]]
  }

  test(s"it should convert to a type") {
    val expectedValue = s"Random-${new Random().nextInt(100)}"
    val payload       = validJson(expectedValue)

    val someResult = implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload).toOption
    someResult.map(_.foo) shouldBe Some(expectedValue)
  }

  test("it should not parse to a type with an invalid json") {
    val payload = invalidJson()
    implicitly[PayloadUnmarshaller[Entity]].unmarshal(payload) shouldBe a[Left[_,_]]
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result      = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).toOption.map(_.noSpaces)
    val expected    = StringPayloadUnmarshaller.unmarshal(jsonPayload).toOption.get
    result shouldBe Some(expected)
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result   = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Entity]).toOption.map(_.asJson.noSpaces)
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).toOption.get

    result shouldBe Some(expected)
  }

  def validJson(value: String): Payload = Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)

  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
