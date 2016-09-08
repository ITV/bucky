package itv.bucky

import argonaut._
import Argonaut._
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class ArgonautPayloadUnmarshallersTest  extends FunSuite {

  import UnmarshalResultOps._
  import ArgonautPayloadUnmarshallers._

  case class Some(foo: String)
  implicit val someCodec: CodecJson[Some] = casecodec1(Some.apply, Some.unapply)("foo")

  test("it should parse a json object") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)
    val jsonResult = toEitherJson.unmarshal(payload).get.valueOr(failure => fail(s"$failure"))
    jsonResult.fieldOr("foo", fail).stringOr(fail) shouldBe expectedValue
  }

  test("it should not parse an invalid json") {
    val payload = invalidJson()
    val jsonResult = toEitherJson.unmarshal(payload).get

    jsonResult.fold(
      failure => failure should include(StringPayloadUnmarshaller.unmarshal(payload).get),
      success => fail(s"it should nor convert: $payload to $success")
    )
  }


  test("it should convert to an specific type") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)

    val someResult = toEither[Some].unmarshal(payload).get.valueOr(failure => fail(s"$failure"))

    someResult.foo shouldBe expectedValue
  }


  test("it should not parse to an specific type with an invalid json") {
    val payload = invalidJson()
    val jsonResult = toEither[Some].unmarshal(payload).get

    jsonResult.fold(
      failure => failure should include(StringPayloadUnmarshaller.unmarshal(payload).get),
      success => fail(s"it should nor convert: $payload to $success")
    )
  }

  test("it should convert to a validated type") {
    val expectedValue = new Random().nextString(100)
    val payload = validJson(expectedValue)

    val someResult = toValidation[Some].unmarshal(payload).get.valueOr(failure => fail(s"$failure"))

    someResult.foo shouldBe expectedValue
  }

  test("it should not parse to a validated type with an invalid json") {
    val payload = invalidJson()
    val jsonResult = toValidation[Some].unmarshal(payload).get

    jsonResult.fold(
      failure => failure should include(StringPayloadUnmarshaller.unmarshal(payload).get),
      success => fail(s"it should nor convert: $payload to $success")
    )
  }

  def validJson(value: String): Payload =  Payload.from(s"""{"foo": "$value"}""")
  def invalidJson() = Payload.from(s"<[Bar: ${new Random().nextInt()}}]")

}
