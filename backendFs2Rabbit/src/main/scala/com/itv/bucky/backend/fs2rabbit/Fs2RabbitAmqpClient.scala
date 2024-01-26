package com.itv.bucky.backend.fs2rabbit

import cats.data.Kleisli
import cats.effect.implicits.genSpawnOps
import cats.effect.{Async, Resource}
import cats.implicits._
import com.itv.bucky
import com.itv.bucky.consume.DeliveryMode
import com.itv.bucky.decl.ExchangeType
import com.itv.bucky.publish.{ContentEncoding, ContentType, PublishCommand}
import com.itv.bucky.{AmqpClient, AmqpClientConfig, Envelope, ExchangeName, Handler, Payload, Publisher, QueueName, RoutingKey, consume, decl, publish}
import com.rabbitmq.client.LongString
import dev.profunktor.fs2rabbit.arguments.SafeArg
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration._
import dev.profunktor.fs2rabbit.effects.{EnvelopeDecoder, MessageEncoder}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{ArrayVal, BooleanVal, ByteArrayVal, ByteVal, DecimalVal, DoubleVal, FloatVal, IntVal, LongVal, NullVal, ShortVal, StringVal, TableVal, TimestampVal}
import dev.profunktor.fs2rabbit.model.{AmqpFieldValue, ShortString, instantOrderWithSecondPrecision}
import scodec.bits.ByteVector

import java.util.Date
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.language.higherKinds

class Fs2RabbitAmqpClient[F[_]](client: RabbitClient[F], connection: model.AMQPConnection, publishChannel: model.AMQPChannel)(implicit F: Async[F])
    extends AmqpClient[F] {

  implicit val deliveryEncoder: MessageEncoder[F, PublishCommand] =
    Kleisli { publishCommand =>
      def toAmqpValue(value: AnyRef): model.AmqpFieldValue = value match {
        case bd: java.math.BigDecimal => DecimalVal.unsafeFrom(bd)
        case ts: java.time.Instant    => TimestampVal.from(ts)
        case d: java.util.Date        => TimestampVal.from(d)
        case t: java.util.Map[_, _] =>
          TableVal(t.asScala.toMap.collect { case (key: String, v: AnyRef) => ShortString.unsafeFrom(key) -> toAmqpValue(v) })
        case byte: java.lang.Byte     => ByteVal(byte)
        case double: java.lang.Double => DoubleVal(double)
        case float: java.lang.Float   => FloatVal(float)
        case short: java.lang.Short   => ShortVal(short)
        case byteArray: Array[Byte]   => ByteArrayVal(ByteVector(byteArray))
        case b: java.lang.Boolean     => BooleanVal(b)
        case i: java.lang.Integer     => IntVal(i)
        case l: java.lang.Long        => LongVal(l)
        case s: java.lang.String      => StringVal(s)
        case ls: LongString           => StringVal(ls.toString)
        case a: java.util.List[_]     => ArrayVal(a.asScala.toVector.collect { case v: AnyRef => toAmqpValue(v) })
        case _                        => NullVal
      }

      val fs2MessageHeaders: Map[String, model.AmqpFieldValue] = publishCommand.basicProperties.headers.view.mapValues(toAmqpValue).toMap

      val message = model.AmqpMessage(
        publishCommand.body.value,
        model.AmqpProperties(
          contentType = publishCommand.basicProperties.contentType.map(_.value),
          contentEncoding = publishCommand.basicProperties.contentEncoding.map(_.value),
          priority = publishCommand.basicProperties.priority,
          deliveryMode = publishCommand.basicProperties.deliveryMode.map(dm => model.DeliveryMode.from(dm.value)),
          correlationId = publishCommand.basicProperties.correlationId,
          messageId = publishCommand.basicProperties.messageId,
          `type` = publishCommand.basicProperties.messageType,
          userId = publishCommand.basicProperties.userId,
          appId = publishCommand.basicProperties.appId,
          expiration = publishCommand.basicProperties.expiration,
          replyTo = publishCommand.basicProperties.replyTo,
          clusterId = publishCommand.basicProperties.clusterId,
          timestamp = publishCommand.basicProperties.timestamp.map(_.toInstant),
          headers = fs2MessageHeaders
        )
      )

      F.pure(message)
    }

  def deliveryDecoder(queueName: QueueName): EnvelopeDecoder[F, consume.Delivery] =
    Kleisli { amqpEnvelope =>
      val messageProperties = publish.MessageProperties(
        contentType = amqpEnvelope.properties.contentType.map(ContentType.apply),
        contentEncoding = amqpEnvelope.properties.contentEncoding.map(ContentEncoding.apply),
        headers = amqpEnvelope.properties.headers.view.mapValues(_.toValueWriterCompatibleJava).toMap,
        deliveryMode = amqpEnvelope.properties.deliveryMode.map(dm => DeliveryMode(dm.value)),
        priority = amqpEnvelope.properties.priority,
        correlationId = amqpEnvelope.properties.correlationId,
        replyTo = amqpEnvelope.properties.replyTo,
        expiration = amqpEnvelope.properties.expiration,
        messageId = amqpEnvelope.properties.messageId,
        timestamp = amqpEnvelope.properties.timestamp.map(Date.from),
        messageType = amqpEnvelope.properties.`type`,
        userId = amqpEnvelope.properties.userId,
        appId = amqpEnvelope.properties.appId,
        clusterId = amqpEnvelope.properties.clusterId
      )

      F.pure(
        consume.Delivery(
          Payload(amqpEnvelope.payload),
          consume.ConsumerTag.create(queueName),
          Envelope(
            amqpEnvelope.deliveryTag.value,
            amqpEnvelope.redelivered,
            ExchangeName(amqpEnvelope.exchangeName.value),
            RoutingKey(amqpEnvelope.routingKey.value)
          ),
          messageProperties
        )
      )
    }

  override def declare(declarations: decl.Declaration*): F[Unit] = declare(declarations.toList)

  override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = {
    def argumentsFromAnyRef(arguments: Map[String, AnyRef]): Map[String, SafeArg] =
      arguments.view.mapValues[SafeArg] {
        case arg: String            => arg
        case arg: BigDecimal        => arg
        case arg: Integer           => arg.intValue()
        case arg: java.lang.Long    => arg.longValue()
        case arg: java.lang.Double  => arg.doubleValue()
        case arg: java.lang.Float   => arg.floatValue()
        case arg: java.lang.Short   => arg.shortValue()
        case arg: java.lang.Boolean => arg.booleanValue()
        case arg: java.lang.Byte    => arg.byteValue()
        case arg: java.util.Date    => arg
        case t                      => throw new IllegalArgumentException(s"Unsupported type for rabbit arguments $t")
      }.toMap

    def exchangeTypeToFs2ExchangeType(exchangeType: ExchangeType): model.ExchangeType =
      exchangeType match {
        case decl.Direct  => model.ExchangeType.Direct
        case decl.Topic   => model.ExchangeType.Topic
        case decl.Headers => model.ExchangeType.Headers
        case decl.Fanout  => model.ExchangeType.FanOut
      }

    implicit val channel: model.AMQPChannel = publishChannel
    declarations.toList.traverse_ {
      case decl.Exchange(name, exchangeType, isDurable, shouldAutoDelete, isInternal, arguments, bindings) =>
        client.declareExchange(
          DeclarationExchangeConfig
            .default(model.ExchangeName(name.value), exchangeTypeToFs2ExchangeType(exchangeType))
            .copy(
              arguments = argumentsFromAnyRef(arguments),
              durable = if (isDurable) Durable else NonDurable,
              autoDelete = if (shouldAutoDelete) AutoDelete else NonAutoDelete,
              internal = if (isInternal) Internal else NonInternal
            )
        ) *>
          declare(bindings)
      case decl.Binding(exchangeName, queueName, routingKey, arguments) =>
        client.bindQueue(
          model.QueueName(queueName.value),
          model.ExchangeName(exchangeName.value),
          model.RoutingKey(routingKey.value),
          model.QueueBindingArgs(argumentsFromAnyRef(arguments))
        )
      case decl.ExchangeBinding(destinationExchangeName, sourceExchangeName, routingKey, arguments) =>
        client.bindExchange(
          model.ExchangeName(destinationExchangeName.value),
          model.ExchangeName(sourceExchangeName.value),
          model.RoutingKey(routingKey.value),
          model.ExchangeBindingArgs(argumentsFromAnyRef(arguments))
        )
      case decl.Queue(name, isDurable, isExclusive, shouldAutoDelete, arguments) =>
        client.declareQueue(
          DeclarationQueueConfig
            .default(model.QueueName(name.value))
            .copy(
              arguments = argumentsFromAnyRef(arguments),
              durable = if (isDurable) Durable else NonDurable,
              autoDelete = if (shouldAutoDelete) AutoDelete else NonAutoDelete,
              exclusive = if (isExclusive) Exclusive else NonExclusive
            )
        )
    }

  }

  override def publisher(): Publisher[F, publish.PublishCommand] = (publishCommand: PublishCommand) => {
    implicit val channel: model.AMQPChannel = publishChannel
    client
      .createPublisher[PublishCommand](model.ExchangeName(publishCommand.exchange.value), model.RoutingKey(publishCommand.routingKey.value))
      .flatMap(f => f(publishCommand))
  }

  override def registerConsumer(
      queueName: bucky.QueueName,
      handler: Handler[F, consume.Delivery],
      exceptionalAction: consume.ConsumeAction,
      prefetchCount: Int,
      shutdownTimeout: FiniteDuration,
      shutdownRetry: FiniteDuration
  ): Resource[F, Unit] =
    client.createChannel(connection).flatMap { implicit channel =>
      implicit val decoder: EnvelopeDecoder[F, consume.Delivery] = deliveryDecoder(queueName)
      Resource.eval(client.createAckerConsumer[consume.Delivery](model.QueueName(queueName.value))).flatMap { case (acker, consumer) =>
        consumer
          .evalMap(delivery => handler(delivery.payload).map(_ -> delivery.deliveryTag))
          .evalMap {
            case (consume.Ack, tag)                => acker(model.AckResult.Ack(tag))
            case (consume.DeadLetter, tag)         => acker(model.AckResult.NAck(tag))
            case (consume.RequeueImmediately, tag) => acker(model.AckResult.Reject(tag))
          }
          .compile
          .drain
          .background
          .map(_ => ())
      }
    }

  override def isConnectionOpen: F[Boolean] = F.pure(connection.value.isOpen)

}

object Fs2RabbitAmqpClient {
  def apply[F[_]](config: AmqpClientConfig)(implicit async: Async[F]): Resource[F, Fs2RabbitAmqpClient[F]] = {
    val fs2RabbitConfig = Fs2RabbitConfig(
      config.host,
      config.port,
      "/",
      10.seconds,
      ssl = false,
      Some(config.username),
      Some(config.password),
      requeueOnNack = false,
      requeueOnReject = true,
      Some(10)
    )
    for {
      client         <- RabbitClient.default[F](fs2RabbitConfig).resource
      connection     <- client.createConnection
      publishChannel <- client.createConnectionChannel
    } yield new Fs2RabbitAmqpClient(client, connection, publishChannel)
  }
}
