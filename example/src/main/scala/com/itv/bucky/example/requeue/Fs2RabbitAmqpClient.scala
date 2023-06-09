package com.itv.bucky.example.requeue

import cats.data.Kleisli
import cats.effect.{IO, Resource}
import cats.implicits._
import com.itv.bucky
import com.itv.bucky.publish.PublishCommand
import com.itv.bucky.{AmqpClient, Envelope, Handler, Payload, Publisher, consume, decl, publish, ExchangeName => BuckyExchangeName, RoutingKey => BuckyRoutingKey}
import com.itv.bucky.decl.{ExchangeType => BuckyExchangeType}
import dev.profunktor.fs2rabbit.arguments.{SafeArg, SafeArgument}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration.{DeclarationExchangeConfig, DeclarationQueueConfig}
import dev.profunktor.fs2rabbit.effects.{EnvelopeDecoder, MessageEncoder}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AMQPConnection, AckResult, AmqpMessage, AmqpProperties, QueueBindingArgs, ExchangeName => Fs2ExchangeName, ExchangeType => Fs2ExchangeType, QueueName => Fs2RabbitQueueName, RoutingKey => Fs2RoutingKey}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.FiniteDuration

class Fs2RabbitAmqpClient private (config: Fs2RabbitConfig, client: RabbitClient[IO], connection: AMQPConnection, publishChannel: AMQPChannel)
    extends AmqpClient[IO] {

  implicit val arrayByteEncoder: MessageEncoder[IO, Array[Byte]] =
    Kleisli { bytes =>
      IO.pure(AmqpMessage(bytes, AmqpProperties.empty.copy(contentEncoding = Some(UTF_8.name()))))
    }

  implicit val arrayByteDecoder: EnvelopeDecoder[IO, Array[Byte]] =
    Kleisli { amqpEnvelope =>
      IO.pure(amqpEnvelope.payload)
    }

  override def declare(declarations: decl.Declaration*): IO[Unit] = declare(declarations.toIterable)

  override def declare(declarations: Iterable[decl.Declaration]): IO[Unit] = {

     def argumentsFromAnyRef(arguments: Map[String, AnyRef]): Map[String, SafeArg] = {
       arguments.mapValues {
         case arg: String => arg
         case arg: BigDecimal => arg
//         case arg: Integer => Int.box(arg) // TODO are these needed?
//         case arg: Long => Long.box(arg)
//         case arg: Double => Double.box(arg)
//         case arg: Float => Float.box(arg)
//         case arg: Short => Short.box(arg)
//         case arg: Boolean => Boolean.box(arg)
//         case arg: Byte => Byte.box(arg)
         case arg: java.util.Date => arg
         case t => throw new IllegalArgumentException(s"Unsupported type for rabbit arguments $t")
       }
     }

    def exchangeTypeToFs2ExchangeType(exchangeType: BuckyExchangeType): Fs2ExchangeType = {
      exchangeType match {
        case decl.Direct => Fs2ExchangeType.Direct
        case decl.Topic => Fs2ExchangeType.Topic
        case decl.Headers => Fs2ExchangeType.Headers
        case decl.Fanout => Fs2ExchangeType.FanOut
      }
    }

    implicit val channel: AMQPChannel = publishChannel
    declarations.toList.traverse_{
      case decl.Exchange(name, exchangeType, isDurable, shouldAutoDelete, isInternal, arguments, bindings) =>
        client.declareExchange(DeclarationExchangeConfig.default(Fs2ExchangeName(name.value), exchangeTypeToFs2ExchangeType(exchangeType)).copy(arguments = argumentsFromAnyRef(arguments))) *>
          declare(bindings)
      case decl.Binding(exchangeName, queueName, routingKey, arguments) =>
        client.bindQueue(Fs2RabbitQueueName(queueName.value), Fs2ExchangeName(exchangeName.value), Fs2RoutingKey(routingKey.value), QueueBindingArgs(argumentsFromAnyRef(arguments)))
      case decl.ExchangeBinding(destinationExchangeName, sourceExchangeName, routingKey, arguments) => ???
      case decl.Queue(name, isDurable, isExclusive, shouldAutoDelete, arguments) =>
        client.declareQueue(DeclarationQueueConfig.default(Fs2RabbitQueueName(name.value)).copy(arguments = argumentsFromAnyRef(arguments)))
    }

  }

  override def publisher(): Publisher[IO, publish.PublishCommand] = (publishCommand: PublishCommand) => {
    implicit val channel: AMQPChannel = publishChannel
    client
      .createPublisher[Array[Byte]](Fs2ExchangeName(publishCommand.exchange.value), Fs2RoutingKey(publishCommand.routingKey.value))
      .flatMap(f => f(publishCommand.body.value))
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
      Resource.eval(client.createAckerConsumer[Array[Byte]](Fs2RabbitQueueName(queueName.value))).flatMap { case (acker, consumer) =>
        consumer
          .evalMap(msg => handler(amqpEnvelopeToDelivery(queueName, msg)).map(_ -> msg.deliveryTag))
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

  private def amqpEnvelopeToDelivery(queueName: bucky.QueueName, msg: model.AmqpEnvelope[Array[Byte]]) =
    consume.Delivery(
      Payload(msg.payload),
      consume.ConsumerTag.create(queueName),
      Envelope(msg.deliveryTag.value, msg.redelivered, BuckyExchangeName(msg.exchangeName.value), BuckyRoutingKey(msg.routingKey.value)),
      publish.MessageProperties.basic // TODO convert from fs2rabbit MessageProperties
    )

  override def isConnectionOpen: IO[Boolean] = IO(connection.value.isOpen)

}

object Fs2RabbitAmqpClient {
  def apply(config: Fs2RabbitConfig): Resource[IO, Fs2RabbitAmqpClient] =
    for {
      client         <- RabbitClient.default[IO](config).resource
      connection     <- client.createConnection
      publishChannel <- client.createConnectionChannel
    } yield new Fs2RabbitAmqpClient(config, client, connection, publishChannel)
}
