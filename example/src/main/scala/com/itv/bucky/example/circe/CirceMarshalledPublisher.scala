package com.itv.bucky.example.circe

import cats.effect.{ExitCode, IO, IOApp}
import com.itv.bucky._
import com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient
import com.itv.bucky.circe.auto._
import com.itv.bucky.decl._
import com.itv.bucky.example.circe.Shared.Person
import com.itv.bucky.publish._
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global

/*
  The only difference between this and itv.bucky.example.marshalling.MarshalledPublisher
  is the way the PayloadMarshaller is defined in itv.bucky.example.circe.Shared!
 */
object CirceMarshalledPublisher extends IOApp {

  object Declarations {
    val queue      = Queue(QueueName("queue.people.circe"))
    val routingKey = RoutingKey("personCircePublisherRoutingKey")
    val exchange   = Exchange(ExchangeName("exchange.person-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val config                             = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  override def run(args: List[String]): IO[ExitCode] =
    JavaBackendAmqpClient[IO](amqpClientConfig).use { client =>
      for {
        _ <- client.declare(Declarations.all)
        publisher = client.publisherOf[Person](Declarations.exchange.name, Declarations.routingKey)
        _ <- publisher(Person("bob", 22))
      } yield ExitCode.Success
    }

}
