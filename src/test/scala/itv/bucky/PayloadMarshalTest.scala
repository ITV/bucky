package itv.bucky

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.Random

class PayloadMarshalTest extends FunSuite {

  test("Can marshal from/to String") {
    val value = Random.alphanumeric.take(10).mkString
    UnmarshalResult.Success(value) shouldBe Payload.from(value).to[String]
  }


}
