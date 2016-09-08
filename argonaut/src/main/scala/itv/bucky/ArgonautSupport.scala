package itv.bucky

import argonaut._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller


object ArgonautSupport {

  def unmarshallerFromDecodeJson[T](implicit decode: DecodeJson[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller.map(Parse.decodeValidation[T]).flatMap(Unmarshaller.liftResult(res =>
      res.fold(UnmarshalResult.Failure(_, None), UnmarshalResult.Success.apply)
    ))

  def marshallerFromEncodeJson[T](implicit encode: EncodeJson[T]): PayloadMarshaller[T] = StringPayloadMarshaller contramap { value =>
    encode.encode(value).nospaces
  }

  implicit object JsonPayloadMarshaller extends PayloadMarshaller[Json] {
    override def apply(json: Json): Payload = Payload.from(json.nospaces)
  }

}

