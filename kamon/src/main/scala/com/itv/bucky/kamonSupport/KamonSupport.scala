package com.itv.bucky.kamonSupport
import java.time.Instant

import cats.effect.{ConcurrentEffect, IO, Resource}
import com.itv.bucky.consume._
import com.itv.bucky.{AmqpClient, Handler, Publisher, QueueName, decl}
import cats.implicits._
import cats._
import kamon.{Kamon, context}
import kamon.context.Context
import kamon.trace.Tracer.SpanBuilder
import kamon.trace.{Span, SpanCustomizer}
import java.nio.charset.Charset

import com.itv.bucky.LoggingAmqpClient.{logFailedHandler, logFailedPublishMessage, logSuccessfulHandler, logSuccessfullPublishMessage}
import com.itv.bucky.publish.PublishCommand

import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.Try

object KamonSupport {

  def apply[F[_]](amqpClient: AmqpClient[F], logging: Boolean, charset: Charset)(implicit F: ConcurrentEffect[F]): AmqpClient[F] =
    new AmqpClient[F] {
      private val config            = Kamon.config()
      private val includePublishRK  = Try { config.getBoolean("kamon.bucky.publish.add-routing-key-as-metric-tag") }.getOrElse(true)
      private val includeConsumehRK = Try { config.getBoolean("kamon.bucky.consume.add-routing-key-as-metric-tag") }.getOrElse(true)

      override def declare(declarations: decl.Declaration*): F[Unit]          = amqpClient.declare(declarations)
      override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = amqpClient.declare(declarations)

      override def publisher(): Publisher[F, PublishCommand] = {
        val originalPublisher: Publisher[F, PublishCommand] = amqpClient.publisher()
        cmd: PublishCommand =>
          (for {
            context       <- F.delay(Kamon.currentContext())
            clientSpan    <- F.delay(context.get(Span.ContextKey))
            operationName <- s"bucky.publish.exchange.${cmd.exchange.value}".pure[F]
            headers       <- headersFrom(context).pure[F]
            spanBuilder   <- F.delay(spanFor(operationName, clientSpan, cmd))
            span          <- F.delay(context.get(SpanCustomizer.ContextKey).customize(spanBuilder).start())
            newCtx        <- F.delay(context.withKey(Span.ContextKey, span))
            scope         <- F.delay(Kamon.storeContext(newCtx))
            newCmd        <- cmd.copy(basicProperties = cmd.basicProperties.copy(headers = cmd.basicProperties.headers ++ headers)).pure[F]
            result        <- originalPublisher(newCmd).attempt
            _             <- F.delay(Kamon.storeContext(context))
            _             <- if (logging) result.fold(logFailedPublishMessage(_, charset, cmd), _ => logSuccessfullPublishMessage(charset, cmd)) else F.unit
            _ <- F.delay(
              result.fold(e => span.addError("bucky.publish.failure", e).tag("result", "error"), _ => span.tag("result", "success"))
            )
            _ <- F.delay(scope.close())
            _ <- F.delay(span.finish())
          } yield result).rethrow
      }

      private def spanFor(operationName: String, clientSpan: Span, cmd: PublishCommand): SpanBuilder = {
        val span = Kamon
          .buildSpan(operationName)
          .asChildOf(clientSpan)
          .withMetricTag("span.kind", "bucky.publish")
          .withMetricTag("component", "bucky")
          .withMetricTag("exchange", cmd.exchange.value)

        if (includePublishRK) {
          span.withMetricTag("rk", cmd.routingKey.value)
        } else {
          span.withTag("rk", cmd.routingKey.value)
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

      override def registerConsumer(queueN: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction, prefetchCount: Int, shutdownTimeout: FiniteDuration = 1.minutes,
                                    shutdownRetry: FiniteDuration = 500.millis): Resource[F, Unit] = {
        val newHandler = (delivery: Delivery) => {
          (for {
            ctxMap      <- F.delay(contextMapFrom(delivery))
            context     <- Kamon.contextCodec().HttpHeaders.decode(ctxMap).pure[F]
            spanBuilder <- consumerSpanFor(queueN, delivery, context).pure[F]
            span        <- F.delay(spanBuilder.start())
            scope       <- F.delay(Kamon.storeContext(context.withKey(Span.ContextKey, span)))
            result      <- handler(delivery).attempt
            end         <- F.delay(Kamon.clock().instant())
            _ <- if (logging)
              result.fold(logFailedHandler(charset, queueN, exceptionalAction, delivery, _), logSuccessfulHandler(charset, queueN, delivery, _))
            else F.unit
            _ <- F.delay(result.leftMap(t => span.addError("bucky.consume.error", t).tag("result", exceptionalAction.toString.toLowerCase)))
            _ <- F.delay(result.map(r => span.tag("result", r.toString.toLowerCase)))
            _ <- F.delay(scope.close())
            _ <- F.delay(span.finish(end))
          } yield result).rethrow
        }
        amqpClient.registerConsumer(queueN, newHandler, exceptionalAction, prefetchCount)
      }

      private def contextMapFrom(delivery: Delivery) = {
        val contextMap = new context.TextMap.Default
        delivery.properties.headers.foreach(t => Try { contextMap.put(t._1, t._2.toString) })
        contextMap
      }

      private def consumerSpanFor(queueName: QueueName, d: Delivery, context: Context): SpanBuilder = {
        val span = Kamon
          .buildSpan(s"bucky.consume.${queueName.value}")
          .asChildOf(context.get(Span.ContextKey))
          .withMetricTag("span.kind", "bucky.consume")
          .withMetricTag("component", "bucky")
          .withMetricTag("exchange", d.envelope.exchangeName.value)
        if (includeConsumehRK) {
          span.withMetricTag("rk", d.envelope.routingKey.value)
        } else {
          span.withTag("rk", d.envelope.routingKey.value)
        }
      }

      override def isConnectionOpen: F[Boolean] = amqpClient.isConnectionOpen
    }

}
