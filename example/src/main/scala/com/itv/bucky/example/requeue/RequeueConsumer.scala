package com.itv.bucky.example.requeue

import cats.data.Kleisli
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl.{Declaration, Direct, Queue}
import com.itv.bucky.{AmqpClientConfig, DeliveryUnmarshaller, Payload, PayloadUnmarshaller, RequeueHandler, RoutingKey, defaultPreFetchCount}
import com.itv.bucky.consume._
import com.itv.bucky.pattern.requeue._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.effects.EnvelopeDecoder
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AckResult, AmqpEnvelope, DeliveryTag, ExchangeName, ExchangeType, QueueName, RabbitConnection}

object RequeueConsumer extends IOApp with StrictLogging {

  object Declarations {
    val queueName: model.QueueName = model.QueueName(s"requeue_string-1")
//    val all: Iterable[Declaration] =
//      requeueDeclarations(queue.name, RoutingKey(queue.name.value), Some(ExchangeName(s"${queue.name.value}.dlx")), Direct, retryAfter = 1.second)
  }

  val config: Config                     = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  val stringToLogRequeueHandler: RequeueHandler[IO, String] =
    RequeueHandler[IO, String] { message: String =>
      IO.delay {
        println(message)

        message match {
          case "requeue"    => Requeue
          case "deadletter" => DeadLetter
          case _            => Ack
        }
      }
    }

  override def run(args: List[String]): IO[ExitCode] = {
    val config: Fs2RabbitConfig = Fs2RabbitConfig(
      amqpClientConfig.host,
      amqpClientConfig.port,
      amqpClientConfig.virtualHost.getOrElse("/"),
      10.seconds,
      ssl = false,
      Some(amqpClientConfig.username),
      Some(amqpClientConfig.password),
      requeueOnNack = false,
      requeueOnReject = false,
      None
    )

    (for {
      amqpClient <- RabbitClient.default[IO](config).resource
      channel <- amqpClient.createConnectionChannel
      _ <- Resource.eval(amqpClient.declareQueue(DeclarationQueueConfig.default(Declarations.queueName).copy(arguments = Map("x-dead-letter-exchange" -> s"${Declarations.queueName.value}.dlx")))(channel))
      _ <- Resource.eval(amqpClient.declareExchange(ExchangeName(s"${Declarations.queueName.value}.dlx"), ExchangeType.FanOut)(channel))
      _ <- Resource.eval(amqpClient.declareQueue(DeclarationQueueConfig.default(QueueName(s"${Declarations.queueName.value}.dlq")))(channel))
      _ <- Resource.eval(amqpClient.bindQueue(QueueName(s"${Declarations.queueName.value}.dlq"), ExchangeName(s"${Declarations.queueName.value}.dlx"), model.RoutingKey("*"))(channel))
//      _          <- Resource.eval(amqpClient.declare(Declarations.all))
      _ <- registerRequeueConsumerOf(amqpClient)(Declarations.queueName, stringToLogRequeueHandler)
    } yield ()).use(_ => IO.never *> IO(ExitCode.Success))
  }

  def registerRequeueConsumerOf[T](client: RabbitClient[IO])(
      queueName: model.QueueName,
      handler: RequeueHandler[IO, T],
      requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
      onHandlerException: RequeueConsumeAction = Requeue,
      unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
      onRequeueExpiryAction: T => IO[ConsumeAction] = (_: T) => IO.pure[ConsumeAction](DeadLetter),
      prefetchCount: Int = defaultPreFetchCount
  )(implicit unmarshaller: PayloadUnmarshaller[T]): Resource[IO, Unit] = {

    implicit val envelopeDecoder: EnvelopeDecoder[IO, T] =
      new Kleisli[IO, AmqpEnvelope[Array[Byte]], T](amqpEnvelope => // TODO could we just use the circe Decoder?
        unmarshaller.unmarshal(Payload(amqpEnvelope.payload)) match {
          case Left(value)  => IO.raiseError(value)
          case Right(value) => IO.pure(value)
        }
      )

    client.createConnectionChannel.flatMap { implicit channel =>
      Resource.eval(client.createAckerConsumer[T](queueName)).flatMap { case (acker, consumer) =>
        consumer
          .evalMap(msg => handler(msg.payload).map(msg.deliveryTag -> _))
          .evalMap {
            case (tag, Ack) => acker(AckResult.Ack(tag))
            case (tag, DeadLetter) => acker(AckResult.NAck(tag))
            case (tag, Requeue) => ???
            case (tag, RequeueImmediately) => ???
          }
          .compile
          .drain
          .background
          .void
      }
    }
  }

  def registerDeliveryRequeueConsumerOf[T]( // TODO decode headers as an option so only need one consumer type?
      queueName: model.QueueName,
      handler: RequeueHandler[IO, T],
      requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
      onHandlerException: RequeueConsumeAction = Requeue,
      unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
      onRequeueExpiryAction: T => IO[ConsumeAction] = (_: T) => IO.pure[ConsumeAction](DeadLetter),
      prefetchCount: Int = defaultPreFetchCount
  )(implicit unmarshaller: DeliveryUnmarshaller[T]): Resource[IO, Unit] = {
    implicit val envelopeDecoder: EnvelopeDecoder[IO, T] =
      new Kleisli[IO, AmqpEnvelope[Array[Byte]], T](amqpEnvelope => // TODO could we just use the circe Decoder?
        unmarshaller.unmarshal(Delivery(Payload(amqpEnvelope.payload), ???, ???, ???)) match {
          case Left(value)  => IO.raiseError(value)
          case Right(value) => IO.pure(value)
        }
      )

    ???
  }

}
