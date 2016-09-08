package itv.bucky

import argonaut._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller


object ArgonautSupport {

  implicit def toUnmarshaller[T](implicit decode: DecodeJson[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller.map(Parse.decodeValidation[T]).flatMap(Unmarshaller.liftResult(res =>
      res.fold(UnmarshalResult.Failure(_, None), UnmarshalResult.Success.apply)
    ))

  implicit def toMarshaller[T](implicit encode: EncodeJson[T]): PayloadMarshaller[T] = StringPayloadMarshaller contramap { value =>
    encode.encode(value).nospaces
  }

}

