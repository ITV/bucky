package com.itv.bucky

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import itv.bucky._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class CircePayloadUnmarshallersTest  extends FunSuite {

  import itv.bucky.UnmarshalResultOps._
  import com.itv.bucky.CirceSupport._

  case class Some(foo: String)

  test("it should parse a json object") {
    val expectedValue = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = unmarshallerFromDecodeJson[Json].unmarshal(payload).success

    jsonResult.hcursor.get[String]("foo") shouldBe Right(expectedValue)
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    val failure = unmarshallerFromDecodeJson[Json].unmarshal(payload).failure

    failure should include("< (line 1, column 1)")
  }

  test("it should convert to a type") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)

    val someResult = unmarshallerFromDecodeJson[Some].unmarshal(payload).success
    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload = invalidJson()
    val jsonResult = unmarshallerFromDecodeJson[Some].unmarshal(payload).failure

    jsonResult should include("< (line 1, column 1)")
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).success.noSpaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Some]).success.asJson.noSpaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success

    result shouldBe expected
  }

  def validJson(value: String): Payload =  Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)
  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
