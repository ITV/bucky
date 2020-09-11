package com.itv.bucky

import cats.effect.Sync

import language.higherKinds
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

import scala.reflect.{ClassTag, classTag}

package object document {

  case class RMQPublishRoute[T: ClassTag: PayloadMarshaller](exchangeName: ExchangeName, routingKey: RoutingKey) {
    def document: String = s"ExchangeName: ${exchangeName.value}\nRoutingKey: ${routingKey.value}\nPayload: ${classTag[T].runtimeClass.getName}"

    def getPublisher[F[_]](amqpClient: AmqpClient[F]): T => F[Unit] =
      amqpClient.publisherOf(exchangeName, routingKey)
  }

  trait Documentation[F[_]] {
    def getRabbitDocs: F[String]
  }

  object Documentation {
    def apply[F[_]: Sync, T](routes: List[RMQPublishRoute[T]]): Documentation[F] = new Documentation[F] {
      override def getRabbitDocs: F[String] =
        Sync[F].delay(
          s"Producers: \n${routes.map(_.document).mkString("\n---------------\n")}"
        )
    }
  }

  trait PayloadSchema[T] {
    val schema: String
    val payloadMarshaller: PayloadMarshaller[T]
  }

  class DocsRoute[F[_]: Sync] extends Http4sDsl[F] {

    def getRoute(f: Documentation[F]): HttpRoutes[F] = {
        HttpRoutes.of[F] {
        case GET -> Root / "docs" =>
          Ok(f.getRabbitDocs)
      }
    }
  }



}
