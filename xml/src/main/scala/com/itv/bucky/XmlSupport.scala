package com.itv.bucky

import PayloadMarshaller.StringPayloadMarshaller
import Unmarshaller.StringPayloadUnmarshaller

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object XmlSupport {

  def unmarshallerToElem: PayloadUnmarshaller[Elem] = StringPayloadUnmarshaller.flatMap {
    Unmarshaller.liftResult { value =>
      Try(XML.loadString(value)) match {
        case Success(elem) => Right(elem)
        case Failure(failure) =>
          Left(UnmarshalFailure(s"Could not convert to xml: '$value' because [${failure.getMessage}]", Some(failure)))
      }
    }
  }

  def marshallerFromElem: PayloadMarshaller[Elem] = StringPayloadMarshaller contramap (_.toString())
}
