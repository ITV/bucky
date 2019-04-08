package com.itv.bucky

import argonaut._
import PayloadMarshaller.StringPayloadMarshaller
import Unmarshaller.StringPayloadUnmarshaller
import cats.implicits._
object ArgonautSupport {

  def unmarshallerFromDecodeJson[T](implicit decode: DecodeJson[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller
      .flatMap(new Unmarshaller[String, T] {
        override def unmarshal(thing: String): UnmarshalResult[T] =
          Parse.decodeEither[T](thing).leftMap(UnmarshalFailure(_))
      })

  def marshallerFromEncodeJson[T](implicit encode: EncodeJson[T]): PayloadMarshaller[T] =
    StringPayloadMarshaller contramap { value =>
      encode.encode(value).nospaces
    }

  implicit object JsonPayloadMarshaller extends PayloadMarshaller[Json] {
    override def apply(json: Json): Payload = Payload.from(json.nospaces)
  }

}
