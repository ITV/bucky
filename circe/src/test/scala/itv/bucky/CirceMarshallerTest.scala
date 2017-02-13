package itv.bucky

import org.scalatest.FunSuite
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import CirceSupport._
import org.scalatest.Matchers._

class CirceMarshallerTest extends FunSuite {

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

}
