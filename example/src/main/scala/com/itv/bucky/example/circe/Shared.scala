package com.itv.bucky.example.circe

import io.circe.{Decoder, Encoder}

object Shared {

  case class Person(name: String, age: Int)

  object Person {
    import io.circe.generic.semiauto._
    implicit val PersonEncoder: Encoder[Person] = deriveEncoder[Person]
    implicit val PersonDecoder: Decoder[Person] = deriveDecoder[Person]
  }

}
