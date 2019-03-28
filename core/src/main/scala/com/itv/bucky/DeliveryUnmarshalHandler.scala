package com.itv.bucky

import cats.Applicative
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds

class DeliveryUnmarshalHandler[F[_], T, S](unmarshaller: DeliveryUnmarshaller[T])(
    handler: T => F[S],
    deserializationFailureAction: S)(implicit F: Applicative[F])
    extends (Delivery => F[S])
    with StrictLogging {
  override def apply(delivery: Delivery): F[S] = unmarshaller.unmarshal(delivery) match {
    case UnmarshalResult.Success(message) => handler(message)
    case UnmarshalResult.Failure(reason, throwable) =>
      F.point {
        logger.error(s"Cannot deserialize: ${delivery.body} because: '$reason' (will $deserializationFailureAction)",
                     throwable)
        deserializationFailureAction
      }
  }
}

class UnmarshalFailureAction[F[_], T](unmarshaller: DeliveryUnmarshaller[T])(implicit F: Applicative[F]) extends StrictLogging {
  def apply(f: T => F[Unit])(delivery: Delivery): F[Unit] = {
    unmarshaller.unmarshal(delivery) match {
      case UnmarshalResult.Success(message) => f(message)
      case UnmarshalResult.Failure(reason, throwable) =>
        F.point {
          logger.error(s"Cannot deserialize: ${delivery.body} because: '$reason'", throwable)
          ()
        }
    }
  }
}