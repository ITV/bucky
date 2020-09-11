package com.itv.bucky

import cats.effect.Sync
import com.itv.bucky.wiring.Wiring
import com.itv.bucky.wiring.implicits._
import language.higherKinds
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

package object document {

  trait Documentation[F[_]] {
    def getRabbitDocs: F[String]
  }

  object Documentation {
    def apply[F[_]: Sync](producers: List[Wiring[_]]): Documentation[F] = new Documentation[F] {
      override def getRabbitDocs: F[String] =
        Sync[F].delay(
          s"Producers: ${producers.map(_.document).mkString("\n---------------\n")}"
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
