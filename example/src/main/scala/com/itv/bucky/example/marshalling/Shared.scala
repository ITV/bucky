package com.itv.bucky.example.marshalling

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.{PayloadMarshaller, UnmarshalResult, Unmarshaller}

object Shared {

  case class Person(name: String, age: Int)

  val personMarshaller: PayloadMarshaller[Person] = StringPayloadMarshaller.contramap(p => s"${p.name},${p.age}")

  val personUnmarshaller = StringPayloadUnmarshaller flatMap Unmarshaller.liftResult { incoming =>
    incoming.split(",") match {
      case Array(name, ageString) if ageString.forall(_.isDigit) =>
        UnmarshalResult.Success(Person(name, ageString.toInt))

      case Array(name, ageNotInteger) =>
        UnmarshalResult.Failure(s"Age was not an integer in '$ageNotInteger'")

      case _ =>
        UnmarshalResult.Failure(s"Expected message to be in format <name>,<age>: got '$incoming'")
    }

  }

}
