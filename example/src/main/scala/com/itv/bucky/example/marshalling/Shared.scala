package com.itv.bucky.example.marshalling

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.{PayloadMarshaller, UnmarshalFailure, UnmarshalResult, Unmarshaller}

object Shared {

  //snippet 1
  case class Person(name: String, age: Int)

  //contramap allows us to build a PayloadMarshaller[Person]
  //in terms of a PayloadMarshaller[String]
  //we just have to provide a function Person => String
  //in this case we comma seperate the name and age of the person
  val personMarshaller: PayloadMarshaller[Person] =
    StringPayloadMarshaller.contramap(p => s"${p.name},${p.age}")
  //end snippet 1

  val personUnmarshaller = StringPayloadUnmarshaller flatMap Unmarshaller.liftResult { incoming =>
    incoming.split(",") match {
      case Array(name, ageString) if ageString.forall(_.isDigit) => Right(Person(name, ageString.toInt))
      case Array(name, ageNotInteger)                            => Left(UnmarshalFailure(s"Age was not an integer in '$ageNotInteger'"))
      case _                                                     => Left(UnmarshalFailure("Expected message to be in format <name>,<age>: got '$incoming'"))
    }
  }

}
