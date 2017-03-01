package com.itv.bucky.example.circe

import com.itv.bucky.CirceSupport._
import com.itv.bucky.{PayloadMarshaller, PayloadUnmarshaller}
import io.circe.generic.auto._

object Shared {

  case class Person(name: String, age: Int)

  val personMarshaller: PayloadMarshaller[Person] =
    marshallerFromEncodeJson

  val personUnmarshaller: PayloadUnmarshaller[Person] =
    unmarshallerFromDecodeJson

}
