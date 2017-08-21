package com.itv.bucky.example.basic

import com.itv.bucky._
import com.itv.bucky.decl.Exchange
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.itv.lifecycle.Lifecycle
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object RawPublisher extends App with StrictLogging {

  //start snippet 1
  val brokerHostname   = ConfigFactory.load("bucky").getString("rmq.host")
  val amqpClientConfig = AmqpClientConfig(brokerHostname, 5672, "guest", "guest")

  object Declarations {
    val exchange   = Exchange(ExchangeName("raw.publisher.exchange"))
    val routingKey = RoutingKey("raw.publish.routingkey")
  }
  case class Person(name: String, age: Int)
  //end snippet 1

  //start snippet 2
  val publisherLifecycle: Lifecycle[Publisher[Future, PublishCommand]] =
    for {
      //create a connection to the broker
      client <- AmqpClientLifecycle(amqpClientConfig)
      //declare the exchange we will publish to
      _ <- DeclarationLifecycle(Seq(Declarations.exchange), client)
      //create an instance of a generic publisher
      publisher <- client.publisher()
    } yield publisher
  //end snippet 2

  //start snippet 3
  Lifecycle.using(publisherLifecycle) { publisher =>
    //create our message
    val message = Person("Bob", 67)
    //convert it to an AMQP payload
    val payload = Payload.from(s"${message.name},${message.age}")
    //build a PublishCommand
    val publishCommand =
      PublishCommand(Declarations.exchange.name, Declarations.routingKey, MessageProperties.basic, payload)
    //publish the command!
    val publishResult: Future[Unit] = publisher(publishCommand)
    //wait for the result
    Await.result(publishResult, 10.seconds)
  }
  //end snippet 3

}
