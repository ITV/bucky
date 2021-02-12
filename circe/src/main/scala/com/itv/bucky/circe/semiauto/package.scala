package com.itv.bucky.circe

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.{Payload, PayloadMarshaller, PayloadUnmarshaller, UnmarshalResult, Unmarshaller}
import io.circe.parser.decode
import io.circe.{Decoder, Encoder, Json}

package object semiauto {

  object JsonPayloadMarshaller extends PayloadMarshaller[Json] {
    override def apply(json: Json): Payload = Payload.from(json.noSpaces)
  }

  def unmarshallerFromDecodeJson[T](implicit decoder: Decoder[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller
      .flatMap[T](new Unmarshaller[String, T] {
        override def unmarshal(thing: String): UnmarshalResult[T] =
          decode[T](thing)
      })

  def marshallerFromEncodeJson[T](implicit encoder: Encoder[T]): PayloadMarshaller[T] =
    StringPayloadMarshaller.contramap { value =>
      encoder(value).noSpaces
    }

}
