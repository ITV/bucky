package com.itv.bucky.natchez

import _root_.natchez.EntryPoint
import _root_.natchez.Span
import _root_.natchez.Tags
import _root_.natchez.Trace
import cats.Monad
import cats.data.Kleisli
import cats.effect.Resource
import cats.effect.implicits._
import cats.effect.kernel.MonadCancelThrow
import cats.effect.kernel.Outcome
import cats.implicits._
import com.itv.bucky.AmqpClient
import com.itv.bucky.Handler
import com.itv.bucky.Publisher
import com.itv.bucky.QueueName
import com.itv.bucky.consume.ConsumeAction
import com.itv.bucky.consume.Delivery
import com.itv.bucky.decl.Declaration
import com.itv.bucky.publish.MessageProperties
import com.itv.bucky.publish.PublishCommand
import natchez.TraceValue

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object TracedAmqpClient {
  def apply[F[_]: Trace: MonadCancelThrow](amqpClient: AmqpClient[F]): AmqpClient[F] =
    new TracedAmqpClient[F](amqpClient)
}

protected[natchez] class TracedAmqpClient[F[_]: Trace: MonadCancelThrow](
    amqpClient: AmqpClient[F]
) extends AmqpClient[F] {

  def declare(declarations: Declaration*): F[Unit] =
    amqpClient.declare(declarations)

  def declare(declarations: Iterable[Declaration]): F[Unit] =
    amqpClient.declare(declarations)

  def publisher(): Publisher[F, PublishCommand] = { command: PublishCommand =>
    val publisher = amqpClient.publisher()

    Trace[F].span("bucky-publish") {
      for {
        _ <- Trace[F].put(commandTraceFields(command, true): _*)
        _ <- guaranteeCaseTrace(publisher(command))
      } yield ()
    }
  }

  def registerConsumer(
      queueName: QueueName,
      handler: Handler[F, Delivery],
      exceptionalAction: ConsumeAction,
      prefetchCount: Int,
      shutdownTimeout: FiniteDuration,
      shutdownRetry: FiniteDuration
  ): Resource[F, Unit] = {
    val tracedHandler = { (delivery: Delivery) =>
      Trace[F].span("bucky-handler") {
        for {
          _   <- Trace[F].put(deliveryTraceFields(delivery, true): _*)
          res <- guaranteeCaseTrace(handler(delivery))
        } yield res
      }
    }

    amqpClient.registerConsumer(
      queueName,
      tracedHandler,
      exceptionalAction,
      prefetchCount,
      shutdownTimeout,
      shutdownRetry
    )
  }

  def isConnectionOpen: F[Boolean] =
    amqpClient.isConnectionOpen

  private def messagePropertiesTraceFields(props: MessageProperties): List[(String, TraceValue)] = {
    val ints = List(
      "delivery_mode" -> props.deliveryMode,
      "priority"      -> props.priority
    ).collect { case (k, Some(v: Int)) => (k, TraceValue.NumberValue(v)) }

    val strings = List(
      "content_type"     -> props.contentType.map(_.value),
      "content_encoding" -> props.contentType.map(_.value),
      "correlation_id"   -> props.correlationId,
      "reply_to"         -> props.replyTo,
      "expiration"       -> props.expiration,
      "message_id"       -> props.messageId,
      "timestamp"        -> props.timestamp.map(_.toString()),
      "message_type"     -> props.messageType,
      "user_id"          -> props.userId,
      "app_id"           -> props.appId,
      "cluster_id"       -> props.clusterId
    ).collect { case (k, Some(v: String)) => (k, TraceValue.StringValue(v)) }

    (ints ++ strings).map { case (k, v) => (s"rabbitmq.${k}", v) }
  }

  private def deliveryTraceFields(del: Delivery, detailedProperties: Boolean): List[(String, TraceValue)] = {
    val base: List[(String, TraceValue)] = List(
      Tags.message_bus.destination(s"${del.envelope.exchangeName}#${del.envelope.routingKey}"),
      "rabbitmq.consumer_tag"  -> TraceValue.StringValue(del.consumerTag.value),
      "rabbitmq.delivery_tag"  -> TraceValue.NumberValue(del.envelope.deliveryTag),
      "rabbitmq.redeliver"     -> TraceValue.BooleanValue(del.envelope.redeliver),
      "rabbitmq.exchange_name" -> TraceValue.StringValue(del.envelope.exchangeName.value),
      "rabbitmq.routing_key"   -> TraceValue.StringValue(del.envelope.routingKey.value)
    )

    val detailed = if (detailedProperties) {
      messagePropertiesTraceFields(del.properties)
    } else {
      List.empty
    }

    base ++ detailed
  }

  private def commandTraceFields(cmd: PublishCommand, detailedProperties: Boolean): List[(String, TraceValue)] = {
    val exchange    = cmd.exchange.value
    val rk          = cmd.routingKey.value
    val destination = s"${exchange}#${rk}"

    val base: List[(String, TraceValue)] = List(
      "component"            -> "bucky",
      "rabbitmq.exchange"    -> exchange,
      "rabbitmq.routing-key" -> rk,
      Tags.message_bus.destination(destination),
      Tags.span.kind("producer")
    )

    val detailed = if (detailedProperties) {
      messagePropertiesTraceFields(cmd.basicProperties)
    } else {
      List.empty
    }

    base ++ detailed
  }

  private def guaranteeCaseTrace[A](fa: F[A]): F[A] =
    fa.guaranteeCase {
      case Outcome.Canceled()    => Trace[F].put("cancelled" -> true, Tags.error(true))
      case Outcome.Errored(e)    => Trace[F].attachError(e)
      case Outcome.Succeeded(fa) => Monad[F].unit
    }

}
