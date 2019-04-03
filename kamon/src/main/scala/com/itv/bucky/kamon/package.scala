package com.itv.bucky
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}

import scala.language.higherKinds

package object kamon {
  implicit class TracedClient[F[_]](amqpClient: AmqpClient[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]) {
    def withKamonSupport: AmqpClient[F] = KamonSupport(amqpClient)
  }

  implicit class TracedResource[F[_]](amqpClient: Resource[F, AmqpClient[F]])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]) {
    def withKamonSupport: Resource[F, AmqpClient[F]] = amqpClient.map(KamonSupport(_))
  }
}
