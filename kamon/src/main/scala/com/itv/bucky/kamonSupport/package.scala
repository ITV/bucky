package com.itv.bucky
import cats.effect.ConcurrentEffect

import scala.language.higherKinds

package object kamonSupport {
  implicit class TracedClient[F[_]](amqpClient: AmqpClient[F]) {
    def withKamonSupport()(implicit F: ConcurrentEffect[F]): AmqpClient[F] = KamonSupport(amqpClient)
  }
}
