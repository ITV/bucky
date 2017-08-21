package com.itv.bucky

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random
import scala.xml.Elem

class XmlPayloadUnmarshallersTest extends FunSuite {
  import UnmarshalResultOps._
  import com.itv.bucky.XmlSupport._

  test("it should parse a xml object") {
    val expectedValue    = new Random().nextInt(10)
    val elem             = <foo><bar>{expectedValue}</bar></foo>
    val payload: Payload = Payload.from(elem.toString)

    val elemResult: Elem = unmarshallerToElem.unmarshal(payload).success

    (elemResult \ "bar").map(_.text.toInt).head shouldBe expectedValue
  }

  test("it should not parse an invalid xml object") {
    val expectedValue    = new Random().nextInt(10)
    val invalidElem      = expectedValue
    val payload: Payload = Payload.from(invalidElem.toString)

    val failure = unmarshallerToElem.unmarshal(payload).failure

    failure should include(invalidElem.toString)
  }

}
