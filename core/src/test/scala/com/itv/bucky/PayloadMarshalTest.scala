package com.itv.bucky

import com.itv.bucky.Unmarshaller._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Random, Try}
import cats.implicits._
class PayloadMarshalTest extends FunSuite {

  test("Can marshal from/to String") {
    val value = Random.alphanumeric.take(10).mkString
    Right(value) shouldBe Payload.from(value).unmarshal[String]
  }

  test("Can marshal from/to Int") {
    implicit val intMarshaller: PayloadMarshaller[Int] = new PayloadMarshaller[Int] {
      override def apply(value: Int): Payload = Payload.from(value.toString)
    }

    val stringToInt: Unmarshaller[String, Int] =
      new Unmarshaller[String, Int] {
        override def unmarshal(thing: String): UnmarshalResult[Int] =
          Try(thing.toInt).toEither
            .leftMap(e => UnmarshalFailure(s"'$thing' was not a valid integer", Some(e)))
      }

    implicit val intUnmarshaller: Unmarshaller[Payload, Int] = StringPayloadUnmarshaller flatMap stringToInt

    val value = Random.nextInt()
    Right(value) shouldBe Payload.from(value).unmarshal[Int]
    "'blah' was not a valid integer" shouldBe Payload.from("blah").unmarshal[Int].left.get.getMessage
  }

}
