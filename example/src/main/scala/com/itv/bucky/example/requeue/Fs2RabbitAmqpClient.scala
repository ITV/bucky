package com.itv.bucky.example.requeue

import cats.data.Kleisli
import cats.effect.{IO, Resource}
import cats.implicits._
import com.itv.bucky
import com.itv.bucky.decl.{ExchangeType => BuckyExchangeType}
import com.itv.bucky.publish.PublishCommand
import com.itv.bucky.{
  AmqpClient,
  Envelope,
  Handler,
  Payload,
  Publisher,
  consume,
  decl,
  publish,
  ExchangeName => BuckyExchangeName,
  QueueName => BuckyQueueName,
  RoutingKey => BuckyRoutingKey
}
import com.rabbitmq.client.LongString
import dev.profunktor.fs2rabbit.arguments.SafeArg
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration.{DeclarationExchangeConfig, DeclarationQueueConfig}
import dev.profunktor.fs2rabbit.effects.{EnvelopeDecoder, MessageEncoder}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.AmqpFieldValue._
import dev.profunktor.fs2rabbit.model.{
  AMQPChannel,
  AMQPConnection,
  AckResult,
  AmqpFieldValue,
  AmqpMessage,
  AmqpProperties,
  QueueBindingArgs,
  ExchangeName => Fs2ExchangeName,
  ExchangeType => Fs2ExchangeType,
  QueueName => Fs2RabbitQueueName,
  RoutingKey => Fs2RoutingKey
}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.FiniteDuration

class Fs2RabbitAmqpClient private (client: RabbitClient[IO], connection: AMQPConnection, publishChannel: AMQPChannel) extends AmqpClient[IO] {

  implicit val deliveryEncoder: MessageEncoder[IO, PublishCommand] =
    Kleisli { publishCommand =>
      val fs2MessageHeaders: Map[String, AmqpFieldValue] = publishCommand.basicProperties.headers.mapValues {
        case bd: java.math.BigDecimal => DecimalVal.unsafeFrom(bd)
        case ts: java.time.Instant    => TimestampVal.from(ts)
        case d: java.util.Date        => TimestampVal.from(d)
//        case t: java.util.Map[String@unchecked, AnyRef@unchecked] =>
//          TableVal(t.asScala.toMap.map { case (key, v) => ShortString.unsafeFrom(key) -> unsafeFrom(v) })
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
//        case a: java.util.List[AnyRef@unchecked] => ArrayVal(a.asScala.toVector.map(unsafeFrom))
        case _ => NullVal
      }

      val message = AmqpMessage(publishCommand.body.value, AmqpProperties.empty.copy(
        contentEncoding = Some(UTF_8.name()),
        headers = fs2MessageHeaders,
        expiration = publishCommand.basicProperties.expiration
      ))

      IO.println(message).as(message)
    }

  def deliveryDecoder(queueName: BuckyQueueName): EnvelopeDecoder[IO, consume.Delivery] =
    Kleisli { amqpEnvelope =>

      val messageProperties = publish.MessageProperties.basic.copy(
        headers = amqpEnvelope.properties.headers.mapValues(_.toValueWriterCompatibleJava)
      )

      IO.pure(
        consume.Delivery(
          Payload(amqpEnvelope.payload),
          consume.ConsumerTag.create(queueName),
          Envelope(
            amqpEnvelope.deliveryTag.value,
            amqpEnvelope.redelivered,
            BuckyExchangeName(amqpEnvelope.exchangeName.value),
            BuckyRoutingKey(amqpEnvelope.routingKey.value)
          ),
          messageProperties
        )
      )
    }

  override def declare(declarations: decl.Declaration*): IO[Unit] = declare(declarations.toIterable)

  override def declare(declarations: Iterable[decl.Declaration]): IO[Unit] = {

    def argumentsFromAnyRef(arguments: Map[String, AnyRef]): Map[String, SafeArg] =
      arguments.mapValues {
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
      }

    def exchangeTypeToFs2ExchangeType(exchangeType: BuckyExchangeType): Fs2ExchangeType =
      exchangeType match {
        case decl.Direct  => Fs2ExchangeType.Direct
        case decl.Topic   => Fs2ExchangeType.Topic
        case decl.Headers => Fs2ExchangeType.Headers
        case decl.Fanout  => Fs2ExchangeType.FanOut
      }

    implicit val channel: AMQPChannel = publishChannel
    declarations.toList.traverse_ {
      case decl.Exchange(name, exchangeType, isDurable, shouldAutoDelete, isInternal, arguments, bindings) =>
        client.declareExchange(
          DeclarationExchangeConfig
            .default(Fs2ExchangeName(name.value), exchangeTypeToFs2ExchangeType(exchangeType))
            .copy(arguments = argumentsFromAnyRef(arguments))
        ) *>
          declare(bindings)
      case decl.Binding(exchangeName, queueName, routingKey, arguments) =>
        client.bindQueue(
          Fs2RabbitQueueName(queueName.value),
          Fs2ExchangeName(exchangeName.value),
          Fs2RoutingKey(routingKey.value),
          QueueBindingArgs(argumentsFromAnyRef(arguments))
        )
      case decl.ExchangeBinding(destinationExchangeName, sourceExchangeName, routingKey, arguments) => ???
      case decl.Queue(name, isDurable, isExclusive, shouldAutoDelete, arguments) =>
        client.declareQueue(DeclarationQueueConfig.default(Fs2RabbitQueueName(name.value)).copy(arguments = argumentsFromAnyRef(arguments)))
    }

  }

  override def publisher(): Publisher[IO, publish.PublishCommand] = (publishCommand: PublishCommand) => {
    implicit val channel: AMQPChannel = publishChannel
    println(publishCommand)
    client
      .createPublisher[PublishCommand](Fs2ExchangeName(publishCommand.exchange.value), Fs2RoutingKey(publishCommand.routingKey.value))
      .flatMap(f => f(publishCommand))
  }

  override def registerConsumer(
      queueName: bucky.QueueName,
      handler: Handler[IO, consume.Delivery],
      exceptionalAction: consume.ConsumeAction,
      prefetchCount: Int,
      shutdownTimeout: FiniteDuration,
      shutdownRetry: FiniteDuration
  ): Resource[IO, Unit] =
    client.createChannel(connection).flatMap { implicit channel =>
      implicit val decoder: EnvelopeDecoder[IO, consume.Delivery] = deliveryDecoder(queueName)
      Resource.eval(client.createAckerConsumer[consume.Delivery](Fs2RabbitQueueName(queueName.value))).flatMap { case (acker, consumer) =>
        consumer
          .evalMap(delivery => handler(delivery.payload).map(_ -> delivery.deliveryTag))
          .evalMap {
            case (consume.Ack, tag)                => acker(AckResult.Ack(tag))
            case (consume.DeadLetter, tag)         => acker(AckResult.NAck(tag))
            case (consume.RequeueImmediately, tag) => ??? // TODO
          }
          .compile
          .drain
          .background
          .void
      }
    }

  override def isConnectionOpen: IO[Boolean] = IO(connection.value.isOpen)

}

object Fs2RabbitAmqpClient {
  def apply(config: Fs2RabbitConfig): Resource[IO, Fs2RabbitAmqpClient] =
    for {
      client         <- RabbitClient.default[IO](config).resource
      connection     <- client.createConnection
      publishChannel <- client.createConnectionChannel
    } yield new Fs2RabbitAmqpClient(client, connection, publishChannel)
}
