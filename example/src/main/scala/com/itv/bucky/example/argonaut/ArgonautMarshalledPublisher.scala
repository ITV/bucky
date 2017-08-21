package com.itv.bucky.example.argonaut

import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.itv.bucky.example.argonaut.Shared.Person
import com.itv.lifecycle.Lifecycle

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory

/*
  The only difference between this and itv.bucky.example.marshalling.MarshalledPublisher
  is the way the PayloadMarshaller is defined in itv.bucky.example.argonaut.Shared!
 */
object ArgonautMarshalledPublisher extends App {

  object Declarations {
    val queue      = Queue(QueueName("queue.people.argonaut"))
    val routingKey = RoutingKey("personArgonautPublisherRoutingKey")
    val exchange   = Exchange(ExchangeName("exchange.person-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val config                             = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key (along with exchange configuration) determine where the message will reach.
    */
  val publisherConfig =
    publishCommandBuilder(Shared.personMarshaller) using Declarations.routingKey using Declarations.exchange.name

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Publisher[Future, Person]] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _          <- DeclarationLifecycle(Declarations.all, amqpClient)
      publisher  <- amqpClient.publisherOf(publisherConfig)
    } yield publisher

  Lifecycle.using(lifecycle) { publisher: Publisher[Future, Person] =>
    val result: Future[Unit] = publisher(Person("Bob", 21))
    Await.result(result, 1.second)
  }

}
