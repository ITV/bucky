package com.itv.bucky

import cats.effect.Sync
import language.higherKinds
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

package object document {

  case class RMQPublishRoute(exchangeName: ExchangeName, routingKey: RoutingKey, schema: String)
  object RMQPublishRoute {
    def document(r: RMQPublishRoute): String = s"ExchangeName: ${r.exchangeName.value}\nRoutingKey: ${r.routingKey.value}\nPayload: ${r.schema}"
  }

  trait Documentation[F[_]] {
    def getRabbitDocs: F[String]
  }

  object Documentation {
    def apply[F[_]: Sync](routes: List[RMQPublishRoute]): Documentation[F] = new Documentation[F] {
      override def getRabbitDocs: F[String] =
        Sync[F].delay(
          s"Producers: ${routes.map(RMQPublishRoute.document).mkString("\n---------------\n")}"
        )
      // Return list of producers
      // Return list of consumers
    }
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
