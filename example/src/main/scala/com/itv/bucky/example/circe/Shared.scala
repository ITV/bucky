package com.itv.bucky.example.circe

import com.itv.bucky.{PayloadMarshaller, PayloadUnmarshaller}

object Shared {

  case class Person(name: String, age: Int)

  //begin snippet 1
  import io.circe.generic.auto._
  import com.itv.bucky.CirceSupport._

  val personMarshaller: PayloadMarshaller[Person] =
    marshallerFromEncodeJson
  //end snippet 1

  val personUnmarshaller: PayloadUnmarshaller[Person] =
    unmarshallerFromDecodeJson

}
