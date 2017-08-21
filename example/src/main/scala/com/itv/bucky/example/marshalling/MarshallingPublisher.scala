package com.itv.bucky.example.marshalling

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.lifecycle.Lifecycle
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.example.marshalling.Shared.Person
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object MarshallingPublisher extends App {

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
  val publisherConfig =
    publishCommandBuilder(personMarshaller)
      .using(Declarations.routingKey)
      .using(Declarations.exchange.name)
  //end snippet 2

  //start snippet 3
  val lifecycle: Lifecycle[Publisher[Future, Person]] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _          <- DeclarationLifecycle(Seq(Declarations.exchange), amqpClient)
      publisher  <- amqpClient.publisherOf(publisherConfig)
    } yield publisher
  //end snippet 3

  //start snippet 4
  Lifecycle.using(lifecycle) { publisher: Publisher[Future, Person] =>
    //build the message
    val message = Person("Bob", 67)
    //publish the message!
    val result: Future[Unit] = publisher(message)
    //wait for the result
    Await.result(result, 1.second)
  }
  //end snippet 4

}
