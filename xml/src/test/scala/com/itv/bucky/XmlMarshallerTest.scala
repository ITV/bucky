package com.itv.bucky

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class XmlMarshallerTest extends AnyFunSuite {

  import com.itv.bucky.XmlSupport._

  test("Can marshall a foo") {
    val marshaller = marshallerFromElem

    val foo = <foo><bar>doo</bar></foo>

    marshaller(foo) shouldBe Payload.from(foo.toString)
  }

}
