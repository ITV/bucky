package com.itv.bucky
import java.nio.charset.{Charset, StandardCharsets}

import cats.effect.ConcurrentEffect

import scala.language.higherKinds

package object kamonSupport {
  implicit class TracedClient[F[_]](amqpClient: AmqpClient[F]) {
    def withKamonSupport(logging: Boolean, charset: Charset = StandardCharsets.UTF_8)(implicit F: ConcurrentEffect[F]): AmqpClient[F] =
      KamonSupport(amqpClient, logging, charset)
  }
}
