package com.itv.bucky

import com.itv.bucky.Unmarshaller._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Random, Try}

class PayloadMarshalTest extends FunSuite {

  test("Can marshal from/to String") {
    val value = Random.alphanumeric.take(10).mkString
    UnmarshalResult.Success(value) shouldBe Payload.from(value).unmarshal[String]
  }

  test("Can marshal from/to Int") {
    implicit val intMarshaller: PayloadMarshaller[Int] = new PayloadMarshaller[Int] {
      override def apply(value: Int): Payload = Payload.from(value.toString)
    }
    import UnmarshalResult._

    val stringToInt: Unmarshaller[String, Int] =
      new Unmarshaller[String, Int] {
        override def unmarshal(thing: String): UnmarshalResult[Int] = {
          val parsed = Try(thing.toInt).toOption
          parsed.fold[UnmarshalResult[Int]](s"'$thing' was not a valid integer".unmarshalFailure)(_.unmarshalSuccess)
        }
      }

    implicit val intUnmarshaller: Unmarshaller[Payload, Int] = StringPayloadUnmarshaller flatMap stringToInt

    val value = Random.nextInt()
    value.unmarshalSuccess shouldBe Payload.from(value).unmarshal[Int]

    "'blah' was not a valid integer".unmarshalFailure shouldBe Payload.from("blah").unmarshal[Int]
  }

}
