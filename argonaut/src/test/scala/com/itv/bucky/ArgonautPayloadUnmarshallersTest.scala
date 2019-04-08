package com.itv.bucky

import argonaut._
import Argonaut._
import PayloadMarshaller.StringPayloadMarshaller
import Unmarshaller.StringPayloadUnmarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class ArgonautPayloadUnmarshallersTest extends FunSuite {

  import com.itv.bucky.test._
  import com.itv.bucky.ArgonautSupport._

  case class Some(foo: String)
  implicit val someCodec: CodecJson[Some] = casecodec1(Some.apply, Some.unapply)("foo")

  test("it should parse a json object") {
    val expectedValue    = s"bar ${new Random().nextInt(10)}"
    val payload: Payload = validJson(expectedValue)
    val jsonResult: Json = unmarshallerFromDecodeJson[Json].unmarshal(payload).right.get

    jsonResult.fieldOr("foo", fail).stringOr(fail) shouldBe expectedValue
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    val failure = unmarshallerFromDecodeJson[Json].unmarshal(payload).left.get

    failure.getMessage should include(StringPayloadUnmarshaller.unmarshal(payload).right.get)
  }

  test("it should convert to a type") {
    val expectedValue = s"Test-${new Random().nextInt().toString.take(10)}"
    val payload       = validJson(expectedValue)

    val someResult = unmarshallerFromDecodeJson[Some].unmarshal(payload).right.get

    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload    = invalidJson()
    val jsonResult = unmarshallerFromDecodeJson[Some].unmarshal(payload).left.get
    jsonResult.getMessage should include(StringPayloadUnmarshaller.unmarshal(payload).right.get)
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result      = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Json]).right.get.nospaces
    val expected    = StringPayloadUnmarshaller.unmarshal(jsonPayload).right.get
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result   = jsonPayload.unmarshal(unmarshallerFromDecodeJson[Some]).right.get.asJson.nospaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).right.get

    result shouldBe expected
  }

  def validJson(value: String): Payload = Payload.from(s"""{"foo":"$value"}""")(StringPayloadMarshaller)
  def invalidJson()                     = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
