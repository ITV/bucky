package itv.bucky.example.circe

import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import itv.bucky.CirceSupport._
import itv.bucky.{PayloadMarshaller, PayloadUnmarshaller}

object Shared {

  case class Person(name: String, age: Int)

  val personMarshaller: PayloadMarshaller[Person] =
    marshallerFromEncodeJson

  val personUnmarshaller: PayloadUnmarshaller[Person] =
    unmarshallerFromDecodeJson

}
