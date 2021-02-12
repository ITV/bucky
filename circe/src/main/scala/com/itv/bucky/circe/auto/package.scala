package com.itv.bucky.circe

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.{Payload, PayloadMarshaller, PayloadUnmarshaller}
import io.circe.{Decoder, Encoder, Json}

package object auto {

  implicit val JsonPayloadMarshaller: PayloadMarshaller[Json] = StringPayloadMarshaller.contramap(_.noSpaces)

  implicit def unmarshallerFromDecodeJson[T](implicit decoder: Decoder[T]): PayloadUnmarshaller[T] =
    semiauto.unmarshallerFromDecodeJson

  implicit def marshallerFromEncodeJson[T](implicit encoder: Encoder[T]): PayloadMarshaller[T] =
    semiauto.marshallerFromEncodeJson
}
