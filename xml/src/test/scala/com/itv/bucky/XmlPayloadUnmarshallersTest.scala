package com.itv.bucky

import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.util.Random
import scala.xml.Elem

class XmlPayloadUnmarshallersTest extends AnyFunSuite {
  import com.itv.bucky.test._
  import com.itv.bucky.XmlSupport._

  test("it should parse a xml object") {
    val expectedValue    = new Random().nextInt(10)
    val elem             = <foo><bar>{expectedValue}</bar></foo>
    val payload: Payload = Payload.from(elem.toString)

    val elemResult: Option[Elem] = unmarshallerToElem.unmarshal(payload).toOption

    elemResult.map(elem => (elem \ "bar").map(_.text.toInt).head) shouldBe Some(expectedValue)
  }

  test("it should not parse an invalid xml object") {
    val expectedValue    = new Random().nextInt(10)
    val invalidElem      = expectedValue
    val payload: Payload = Payload.from(invalidElem.toString)

    val failure = unmarshallerToElem.unmarshal(payload).swap.getOrElse(fail())
    
    failure.getMessage should include(invalidElem.toString)
  }

}
