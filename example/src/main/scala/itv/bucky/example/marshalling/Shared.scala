package itv.bucky.example.marshalling

import itv.bucky.{PayloadMarshaller, UnmarshalResult, Unmarshaller}
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller

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
