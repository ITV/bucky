package itv.bucky

import io.circe._
import io.circe.parser.decode
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller


object CirceSupport {

  def unmarshallerFromDecodeJson[T](implicit decoder: Decoder[T]): PayloadUnmarshaller[T] =
    StringPayloadUnmarshaller.map(decode[T]).flatMap(Unmarshaller.liftResult(res =>
      res.fold(error => UnmarshalResult.Failure(error.getMessage, Option(error)), UnmarshalResult.Success.apply)
    ))

  def marshallerFromEncodeJson[T](implicit encoder: Encoder[T]): PayloadMarshaller[T] =
    StringPayloadMarshaller.contramap { value =>
      encoder(value).noSpaces
    }

  implicit object JsonPayloadMarshaller extends PayloadMarshaller[Json] {
    override def apply(json: Json): Payload = Payload.from(json.noSpaces)
  }

}

