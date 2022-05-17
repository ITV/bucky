package com.itv.bucky

import _root_.natchez.Trace
import _root_.natchez.Tags
import cats.implicits._
import cats.effect.implicits._
import scala.language.higherKinds
import com.itv.bucky.decl.Declaration
import com.itv.bucky.publish.PublishCommand
import cats.effect.Resource
import com.itv.bucky.consume.{ConsumeAction, Delivery}
import scala.concurrent.duration.FiniteDuration
import cats.data.Kleisli
import cats.effect.kernel.MonadCancel
import cats.effect.kernel.Outcome

package object natchez {
  implicit class NatchezTracedClient[F[_]: Trace](amqpClient: AmqpClient[F])(implicit M: MonadCancel[F, Throwable]) {

    def traced: AmqpClient[F] = new AmqpClient[F] {
      def declare(declarations: Declaration*): F[Unit] =
        amqpClient.declare(declarations)

      def declare(declarations: Iterable[Declaration]): F[Unit] =
        amqpClient.declare(declarations)

      def publisher(): Publisher[F, PublishCommand] =
        Kleisli { command: PublishCommand =>
          val publisher = amqpClient.publisher()

          val addCommandFields = Trace[F].put(
            "exchange"    -> command.exchange.value,
            "routing-key" -> command.routingKey.value,
            Tags.message_bus.destination(s"${command.exchange.value}#${command.routingKey.value}")
          )

          publisher(command).guaranteeCase {
            case Outcome.Canceled()    => addCommandFields *> Trace[F].put("cancelled" -> true, Tags.error(true))
            case Outcome.Errored(e)    => addCommandFields *> Trace[F].put(Tags.error(true), "error.message" -> e.getMessage())
            case Outcome.Succeeded(fa) => addCommandFields
          }
        }.run

      def registerConsumer(
          queueName: QueueName,
          handler: Handler[F, Delivery],
          exceptionalAction: ConsumeAction,
          prefetchCount: Int,
          shutdownTimeout: FiniteDuration,
          shutdownRetry: FiniteDuration
      ): Resource[F, Unit] =
        amqpClient.registerConsumer(
          queueName,
          handler,
          exceptionalAction,
          prefetchCount,
          shutdownTimeout,
          shutdownRetry
        )

      def isConnectionOpen: F[Boolean] =
        amqpClient.isConnectionOpen

    }
  }
}
