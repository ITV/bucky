package com.itv.bucky.example.marshalling

import cats.effect.{ExitCode, IO, IOApp}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl._
import com.itv.bucky.example.marshalling.Shared.Person
import com.itv.bucky.publish._
import com.typesafe.config.ConfigFactory

import scala.concurrent._
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

object MarshallingPublisher extends IOApp {
  import com.itv.bucky._

  //start snippet 1
  val brokerHostname                     = ConfigFactory.load("bucky").getString("rmq.host")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(brokerHostname, 5672, "guest", "guest")

  object Declarations {
    val routingKey = RoutingKey("personPublisherRoutingKey")
    val exchange   = Exchange(ExchangeName("exchange.person-publisher"))
  }
  case class Person(name: String, age: Int)
  //end snippet 1

  //start snippet 2
  val personMarshaller: PayloadMarshaller[Person] =
    StringPayloadMarshaller.contramap(p => s"${p.name},${p.age}")
  implicit val publisherConfig: PublishCommandBuilder.Builder[Person] =
    publishCommandBuilder(personMarshaller)
      .using(Declarations.routingKey)
      .using(Declarations.exchange.name)
  //end snippet 2

  //start snippet 3
  override def run(args: List[String]): IO[ExitCode] =
    AmqpClient[IO](amqpClientConfig).use { client =>
      for {
        _ <- client.declare(Seq(Declarations.exchange))
        publisher = client.publisherOf[Person]
        _ <- publisher(Person("Bob", 67))
      } yield ExitCode.Success
    }
  //end snippet 3

}
