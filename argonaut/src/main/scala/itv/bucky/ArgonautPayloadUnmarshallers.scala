package itv.bucky

import argonaut.{DecodeJson, Json, JsonParser, Parse}
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller

import scalaz.{Validation, \/}


object ArgonautPayloadUnmarshallers {

  val toEitherJson: PayloadUnmarshaller[String \/ Json] =
    StringPayloadUnmarshaller.map(JsonParser.parse)

  def toEither[T](implicit decode: DecodeJson[T]): PayloadUnmarshaller[String \/ T] =
    StringPayloadUnmarshaller.map(Parse.decodeEither[T])

  def toValidation[T](implicit decode: DecodeJson[T]): PayloadUnmarshaller[Validation[String, T]] =
    StringPayloadUnmarshaller.map(Parse.decodeValidation[T])

}
