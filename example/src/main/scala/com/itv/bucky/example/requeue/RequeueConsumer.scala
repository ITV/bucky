package com.itv.bucky.example.requeue

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl._
import com.itv.bucky._
import com.itv.bucky.consume._
import com.itv.bucky.pattern.requeue._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig

object RequeueConsumer extends IOApp with StrictLogging {

  object Declarations {
    val queue = Queue(QueueName(s"requeue_string-1"))
    val all: Iterable[Declaration] = requeueDeclarations(queue.name, RoutingKey(queue.name.value), Some(ExchangeName(s"${queue.name.value}.dlx")), Direct, retryAfter = 1.second)
  }

  val config: Config = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")
  val fs2RabbitConfig: Fs2RabbitConfig = Fs2RabbitConfig(
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

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      amqpClient <- Fs2RabbitAmqpClient(fs2RabbitConfig)
      _ <- Resource.eval(amqpClient.declare(Declarations.all))
      _ <- amqpClient.registerRequeueConsumerOf(Declarations.queue.name,
        stringToLogRequeueHandler, requeuePolicy = RequeuePolicy(10, 5.seconds))
    } yield ()).use(_ => IO.never *> IO(ExitCode.Success))
}