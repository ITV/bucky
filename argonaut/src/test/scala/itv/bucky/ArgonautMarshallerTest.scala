package itv.bucky

import org.scalatest.FunSuite
import argonaut._
import Argonaut._
import ArgonautSupport._
import org.scalatest.Matchers._

class ArgonautMarshallerTest extends FunSuite {

  case class Foo(bar: String)

  test("Can marshall a foo") {
    import Foo._
    val marshaller: PayloadMarshaller[Foo] = toMarshaller

    val foo = Foo("bar")
    marshaller(foo) shouldBe Payload.from(foo.asJson)
  }

  test("Can marshall a json") {
    val json: Json = Foo("bar").asJson

    val marshaller = implicitly[PayloadMarshaller[Json]]
    marshaller(json) shouldBe Payload.from(json)
  }

  object Foo {
    implicit val codec: CodecJson[Foo] = casecodec1(Foo.apply, Foo.unapply)("bar")
  }

}
