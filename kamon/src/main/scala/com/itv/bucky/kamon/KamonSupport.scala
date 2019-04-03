package com.itv.bucky.kamon
import java.time.Instant

import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import com.itv.bucky.consume._
import com.itv.bucky.{AmqpClient, Handler, Publisher, QueueName, decl}
import cats.implicits._
import cats._
import kamon.{Kamon, context}
import kamon.context.Context
import kamon.trace.Span
import kamon.trace.Tracer.SpanBuilder

import scala.language.higherKinds

object KamonSupport {

  def apply[F[_]](amqpClient: AmqpClient[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): AmqpClient[F] =
    new AmqpClient[F] {
      val includePublishRK = true

      override def declare(declarations: decl.Declaration*): F[Unit]          = amqpClient.declare(declarations)
      override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = amqpClient.declare(declarations)

      override def publisher(): Publisher[F, PublishCommand] = {
        val originalPublisher: Publisher[F, PublishCommand] = amqpClient.publisher()
        cmd: PublishCommand =>
          (for {
            start         <- F.delay(Kamon.clock().instant())
            ctx           <- F.delay(Kamon.currentContext())
            operationName <- s"bucky.publish.exchange.${cmd.exchange.value}".pure[F]
            spanBuilder   <- F.delay(spanFor(start, operationName, ctx, cmd))
            headers       <- headersFrom(ctx).pure[F]
            span          <- F.delay(spanBuilder.start())
            newCmd        <- cmd.copy(basicProperties = cmd.basicProperties.copy(headers = cmd.basicProperties.headers ++ headers)).pure[F]
            result        <- originalPublisher(newCmd).attempt
            end           <- F.delay(Kamon.clock().instant())
            _ <- F.delay(
              result
                .leftMap(t => span.addError("bucky.publish.failure", t).tag("bucky.publish.result", "error"))
                .map(_ => span.tag("bucky.publish.result", "success")))
            _ <- F.delay(span.finish(end))
          } yield result).rethrow
      }

      private def spanFor(now: Instant, operationName: String, ctx: Context, cmd: PublishCommand): SpanBuilder = {
        val span = Kamon
          .buildSpan(operationName)
          .asChildOf(ctx.get(Span.ContextKey))
          .withFrom(now)
          .withMetricTag("span.kind", "client")
          .withMetricTag("component", "bucky.publish")
          .withMetricTag("exchange", cmd.exchange.value)

        if (includePublishRK) {
          span.withMetricTag("rk", cmd.exchange.value)
        } else {
          span.withTag("rk", cmd.exchange.value)
        }
      }

      private def headersFrom(ctx: Context): Map[String, AnyRef] =
        Kamon
          .contextCodec()
          .HttpHeaders
          .encode(ctx)
          .values
          .map { case (key, value) => (key, value) }
          .toMap[String, AnyRef]

      override def registerConsumer(queueName: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction): F[Unit] = {
        val newHandler = (delivery: Delivery) => {
          (for {
            start       <- F.delay(Kamon.clock().instant())
            context     <- Kamon.contextCodec().HttpHeaders.decode(new context.TextMap.Default).pure[F]
            spanBuilder <- consumerSpanFor(queueName, delivery, context, start).pure[F]
            span        <- F.delay(spanBuilder.start())
            result      <- handler(delivery).attempt
            end         <- F.delay(Kamon.clock().instant())
            _           <- F.delay(result.leftMap(t => span.addError("bucky.consume.error", t).tag("result", exceptionalAction.toString.toLowerCase)))
            _           <- F.delay(result.map(r => span.tag("result", r.toString.toLowerCase)))
            _           <- F.delay(span.finish(end))
          } yield result).rethrow
        }
        amqpClient.registerConsumer(queueName, newHandler, exceptionalAction)
      }

      private def consumerSpanFor(queueName: QueueName, d: Delivery, context: Context, start: Instant): SpanBuilder =
        Kamon
          .buildSpan(s"bucky.consume.${queueName.value}")
          .asChildOf(context.get(Span.ContextKey))
          .withFrom(start)
          .withMetricTag("span.kind", "consume")
          .withMetricTag("component", "bucky.consumer")
          .withTag("rk", d.envelope.routingKey.value)

    }

}
