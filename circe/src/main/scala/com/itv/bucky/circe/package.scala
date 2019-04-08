package com.itv.bucky

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import io.circe.{Decoder, Encoder, Json}
import cats.implicits._
import io.circe._
import io.circe.parser.decode

package object circe {

  implicit object JsonPayloadMarshaller extends PayloadMarshaller[Json] {
    override def apply(json: Json): Payload = Payload.from(json.noSpaces)
  }

  implicit def unmarshallerFromDecodeJson[T](implicit decoder: Decoder[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller
      .flatMap[T](new Unmarshaller[String, T] {
      override def unmarshal(thing: String): UnmarshalResult[T] =
        decode[T](thing)
    })

  implicit def marshallerFromEncodeJson[T](implicit encoder: Encoder[T]): PayloadMarshaller[T] =
    StringPayloadMarshaller.contramap { value =>
      encoder(value).noSpaces
    }

}
