package itv.bucky

import argonaut._
import Argonaut._
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class ArgonautPayloadUnmarshallersTest  extends FunSuite {

  import UnmarshalResultOps._
  import ArgonautSupport._

  case class Some(foo: String)
  implicit val someCodec: CodecJson[Some] = casecodec1(Some.apply, Some.unapply)("foo")

  test("it should parse a json object") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)

    val jsonResult = toUnmarshaller[Json].unmarshal(payload).success
    jsonResult.fieldOr("foo", fail).stringOr(fail) shouldBe expectedValue
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    val failure = toUnmarshaller[Json].unmarshal(payload).failure

    failure should include(StringPayloadUnmarshaller.unmarshal(payload).success)
  }

  test("it should convert to a type") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)

    val someResult = toUnmarshaller[Some].unmarshal(payload).success

    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a type with an invalid json") {
    val payload = invalidJson()
    val jsonResult = toUnmarshaller[Some].unmarshal(payload).failure

    jsonResult should include(StringPayloadUnmarshaller.unmarshal(payload).success)
  }

  test("can implicitly unmarshal a json") {
    val jsonPayload = validJson("Hello")
    val result = jsonPayload.unmarshal[Json].success.nospaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success
    result shouldBe expected
  }

  test("can implicitly unmarshal a type") {
    val jsonPayload = validJson("Hello")

    val result = jsonPayload.unmarshal[Some].success.asJson.nospaces
    val expected = StringPayloadUnmarshaller.unmarshal(jsonPayload).success

    result shouldBe expected
  }

  def validJson(value: String): Payload =  Payload.from(s"""{"foo":"$value"}""")
  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
