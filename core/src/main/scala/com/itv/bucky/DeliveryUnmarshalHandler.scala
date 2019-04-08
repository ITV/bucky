package com.itv.bucky

import cats.Applicative
import com.itv.bucky.consume.Delivery
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds

class DeliveryUnmarshalHandler[F[_], T, S](unmarshaller: DeliveryUnmarshaller[T])(handler: T => F[S], deserializationFailureAction: S)(
    implicit F: Applicative[F])
    extends (Delivery => F[S])
    with StrictLogging {
  override def apply(delivery: Delivery): F[S] = unmarshaller.unmarshal(delivery) match {
    case Right(message) => handler(message)
    case Left(throwable) =>
      F.point {
        logger.error(s"Cannot deserialize: ${delivery.body} (will $deserializationFailureAction)", throwable)
        deserializationFailureAction
      }
  }
}

class UnmarshalFailureAction[F[_], T](unmarshaller: DeliveryUnmarshaller[T])(implicit F: Applicative[F]) extends StrictLogging {
  def apply(f: T => F[Unit])(delivery: Delivery): F[Unit] =
    unmarshaller.unmarshal(delivery) match {
      case Right(message) => f(message)
      case Left(throwable) =>
        F.point {
          logger.error(s"Cannot deserialize: ${delivery.body}", throwable)
          ()
        }
    }
}
