package com.itv.bucky

import argonaut._
import Argonaut._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import itv.bucky._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class ArgonautPayloadUnmarshallersTest  extends FunSuite {

  import itv.bucky.UnmarshalResultOps._
  import com.itv.bucky.ArgonautSupport._

  case class Some(foo: String)
  implicit val someCodec: CodecJson[Some] = casecodec1(Some.apply, Some.unapply)("foo")

  test("it should parse a json object") {
    val expectedValue = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = new UnmarshalResultOps(unmarshallerFromDecodeJson[Json].unmarshal(payload)).success

    jsonResult.fieldOr("foo", fail).stringOr(fail) shouldBe expectedValue
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    val failure = unmarshallerFromDecodeJson[Json].unmarshal(payload).failure

    failure should include(StringPayloadUnmarshaller.unmarshal(payload).success)
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

    jsonResult should include(StringPayloadUnmarshaller.unmarshal(payload).success)
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).success.nospaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Some]).success.asJson.nospaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success

    result shouldBe expected
  }

  def validJson(value: String): Payload =  Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)
  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
