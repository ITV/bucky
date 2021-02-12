package com.itv.bucky

import argonaut.Argonaut._
import argonaut._
import com.itv.bucky.ArgonautSupport._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._


class ArgonautMarshallerTest extends AnyFunSuite {

  case class Foo(bar: String)

  test("Can marshall a foo") {
    import Foo._
    val marshaller: PayloadMarshaller[Foo] = marshallerFromEncodeJson

    val foo = Foo("bar")
    marshaller(foo) shouldBe Payload.from(foo.asJson)
  }

  test("Can marshall a json") {
    val json: Json = Foo("bar").asJson

    val marshaller = implicitly[PayloadMarshaller[Json]]
    marshaller(json) shouldBe Payload.from(json)
  }

  object Foo {
    implicit val codec: CodecJson[Foo] = casecodec1(Foo.apply, foo => Some(foo.bar))("bar")
  }

}
