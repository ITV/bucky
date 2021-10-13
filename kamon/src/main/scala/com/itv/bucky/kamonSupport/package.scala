package com.itv.bucky
import cats.effect.kernel.Async

import java.nio.charset.{Charset, StandardCharsets}
import scala.language.higherKinds

package object kamonSupport {
  implicit class TracedClient[F[_]](amqpClient: AmqpClient[F]) {
    def withKamonSupport(logging: Boolean, charset: Charset = StandardCharsets.UTF_8)(implicit F: Async[F]): AmqpClient[F] =
      KamonSupport(amqpClient, logging, charset)
  }
}
