package com.itv.bucky.example.circe

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.itv.bucky.AmqpClient
import com.itv.bucky._
import com.itv.bucky.circe.auto._
import com.itv.bucky.consume.{Ack, Handler}
import com.itv.bucky.decl._
import com.itv.bucky.example.circe.Shared.Person
import com.typesafe.config.{Config, ConfigFactory}
import cats.effect._
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.Implicits.global

/*
  The only difference between this and itv.bucky.example.marshalling.UnmarshalledConsumer
  is the way the PayloadUnmarshaller is defined in itv.bucky.example.circe.Shared!
 */
object CirceUnmarshalledConsumer extends IOApp {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  object Declarations {
    val queue = Queue(QueueName("queue.people.circe"))
    val all   = List(queue)
  }

  val config: Config                     = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  val personHandler: Handler[IO, Person] =
    Handler[IO, Person] { (message: Person) =>
        logger.info(s"${message.name} is ${message.age} years old").as(Ack)
    }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      client <- AmqpClient[IO](amqpClientConfig)
      _      <- Resource.liftF(client.declare(Declarations.all))
      _      <- client.registerConsumerOf(Declarations.queue.name, personHandler)
    } yield ()).use(_ => IO.never *> IO(ExitCode.Success))

}
