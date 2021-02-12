package com.itv.bucky

import com.itv.bucky.circe.auto._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class CirceMarshallerTest extends AnyFunSuite {

  case class Foo(bar: String)

  implicit val fooEncoder: Encoder[Foo] = deriveEncoder[Foo]

  test("Can marshall a foo") {
    val marshaller: PayloadMarshaller[Foo] = marshallerFromEncodeJson

    val foo = Foo("bar")
    marshaller(foo) shouldBe Payload.from(foo.asJson)(JsonPayloadMarshaller)
  }

  test("Can marshall a json") {
    val json: Json = Foo("bar").asJson

    val marshaller = implicitly[PayloadMarshaller[Json]]
    marshaller(json) shouldBe Payload.from(json)
  }

}
