package com.itv.bucky

import cats.syntax.functor._
import cats.{Applicative, Functor}
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery}
import org.typelevel.log4cats.Logger

import scala.language.higherKinds

class DeliveryUnmarshalHandler[F[_]: Logger: Functor, T, S](unmarshaller: DeliveryUnmarshaller[T])(handler: T => F[S], deserializationFailureAction: S)(
    implicit F: Applicative[F], logger: Logger[F])
    extends (Delivery => F[S]) {
  override def apply(delivery: Delivery): F[S] = unmarshaller.unmarshal(delivery) match {
    case Right(message) => handler(message)
    case Left(throwable) =>
      logger
        .error(throwable)(s"Cannot deserialize: ${delivery.body} (will $deserializationFailureAction)")
        .as(deserializationFailureAction)
    }
}


class UnmarshalFailureAction[F[_], T](unmarshaller: DeliveryUnmarshaller[T])(implicit F: Applicative[F], logger: Logger[F]) {
  def apply(f: T => F[ConsumeAction])(delivery: Delivery): F[ConsumeAction] =
    unmarshaller.unmarshal(delivery) match {
      case Right(message) => f(message)
      case Left(throwable) =>
          logger
            .error(throwable)(s"Cannot deserialize: ${delivery.body}")
            .as(DeadLetter)
    }
}
