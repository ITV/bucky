package itv.bucky

import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object XmlSupport {

  def unmarshallerToElem: PayloadUnmarshaller[Elem] = StringPayloadUnmarshaller.flatMap {
      Unmarshaller.liftResult { value =>
          Try(XML.loadString(value)) match {
            case Success(elem) => UnmarshalResult.Success.apply(elem)
            case Failure(failure) => UnmarshalResult.Failure.apply(s"Could not convert to xml: '$value' because [${failure.getMessage}]", Some(failure))
          }
      }
    }

  def marshallerFromElem: PayloadMarshaller[Elem] = StringPayloadMarshaller contramap(_.toString())
}
