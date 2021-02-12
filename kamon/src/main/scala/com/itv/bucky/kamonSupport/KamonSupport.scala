package com.itv.bucky.kamonSupport
import java.nio.charset.Charset

import cats.effect.{ConcurrentEffect, Resource}
import cats.implicits._
import _root_.com.itv.bucky.LoggingAmqpClient.{logFailedHandler, logFailedPublishMessage, logSuccessfulHandler, logSuccessfullPublishMessage}
import com.itv.bucky.consume._
import com.itv.bucky.publish.PublishCommand
import com.itv.bucky.{AmqpClient, Handler, Publisher, QueueName, decl}
import kamon.Kamon
import kamon.context.{Context, HttpPropagation}
import kamon.tag.TagSet
import kamon.trace.{Span, SpanBuilder}
import org.typelevel.log4cats.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.Try

object KamonSupport {

  def apply[F[_]](amqpClient: AmqpClient[F], logging: Boolean, charset: Charset)(implicit F: ConcurrentEffect[F], logger: Logger[F]): AmqpClient[F] =
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
            clientSpan    <- F.delay(context.get(Span.Key))
            operationName <- s"bucky.publish.exchange.${cmd.exchange.value}".pure[F]
            spanBuilder   <- F.delay(spanFor(operationName, clientSpan, cmd))
            span          <- F.delay(spanBuilder.start())
            newCtx        <- F.delay(context.withEntry(Span.Key, span))
            scope         <- F.delay(Kamon.storeContext(newCtx))
            headers       <- headersFrom(newCtx).pure[F]
            newCmd        <- cmd.copy(basicProperties = cmd.basicProperties.copy(headers = cmd.basicProperties.headers ++ headers)).pure[F]
            result        <- originalPublisher(newCmd).attempt
            _             <- F.delay(scope.close())
            _             <- if (logging) result.fold(logFailedPublishMessage(_, charset, cmd), _ => logSuccessfullPublishMessage(charset, cmd)) else F.unit
            _ <- F.delay(
              result.fold(e => span.fail("bucky.publish.failure", e).tag("result", "error"), _ => span.tag("result", "success"))
            )
            _ <- F.delay(span.finish())
          } yield result).rethrow
      }

      private def spanFor(operationName: String, clientSpan: Span, cmd: PublishCommand): SpanBuilder = {
        val span = Kamon.spanBuilder(operationName)
          .asChildOf(clientSpan)
          .tagMetrics(TagSet.from(Map(
            "component" -> "bucky",
            "span.kind" ->"bucky.publish",
            "exchange" -> cmd.exchange.value
          )))

        if (includePublishRK) {
          span.tagMetrics("rk", cmd.routingKey.value)
        } else {
          span.tag("rk", cmd.routingKey.value)
        }
      }

      private def headersFrom(ctx: Context): Map[String, AnyRef] = {
        def headerWriterFromMap(map: mutable.Map[String, String]): HttpPropagation.HeaderWriter = (header: String, value: String) => map.put(header, value)

        val headers = mutable.Map.empty[String, String]
        Kamon.defaultHttpPropagation().write(ctx, headerWriterFromMap(headers))
        headers.toMap
      }

      override def registerConsumer(queueN: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction, prefetchCount: Int, shutdownTimeout: FiniteDuration = 1.minutes,
                                    shutdownRetry: FiniteDuration = 500.millis): Resource[F, Unit] = {
        val newHandler = (delivery: Delivery) => {
          (for {
            context      <- F.delay(contextFrom(delivery))
            spanBuilder <- consumerSpanFor(queueN, delivery, context).pure[F]
            span        <- F.delay(spanBuilder.start())
            scope       <- F.delay(Kamon.storeContext(context.withEntry(Span.Key, span)))
            result      <- handler(delivery).attempt
            end         <- F.delay(Kamon.clock().instant())
            _ <- if (logging)
              result.fold(logFailedHandler(charset, queueN, exceptionalAction, delivery, _), logSuccessfulHandler(charset, queueN, delivery, _))
            else F.unit
            _ <- F.delay(result.leftMap(t => span.fail("bucky.consume.error", t).tag("result", exceptionalAction.toString.toLowerCase)))
            _ <- F.delay(result.map(r => span.tag("result", r.toString.toLowerCase)))
            _ <- F.delay(scope.close())
            _ <- F.delay(span.finish(end))
          } yield result).rethrow
        }
        amqpClient.registerConsumer(queueN, newHandler, exceptionalAction, prefetchCount)
      }

      private def contextFrom(delivery: Delivery): Context =
        Kamon.defaultHttpPropagation().read(headerReaderFromMap(delivery.properties.headers.mapValues(_.toString)))

      def headerReaderFromMap(map: Map[String, String]): HttpPropagation.HeaderReader = new HttpPropagation.HeaderReader {
        override def read(header: String): Option[String] = map.get(header)
        override def readAll(): Map[String, String] = map
      }

      private def consumerSpanFor(queueName: QueueName, d: Delivery, context: Context): SpanBuilder = {
        val span = Kamon
          .spanBuilder(s"bucky.consume.${queueName.value}")
          .asChildOf(context.get(Span.Key))
          .tagMetrics(TagSet.from(Map(
            "component" -> "bucky",
            "span.kind" -> "bucky.consume",
            "exchange" -> d.envelope.exchangeName.value
          )))

        if (includeConsumehRK) {
          span.tagMetrics("rk", d.envelope.routingKey.value)
        } else {
          span.tag("rk", d.envelope.routingKey.value)
        }
      }

      override def isConnectionOpen: F[Boolean] = amqpClient.isConnectionOpen
    }

}
