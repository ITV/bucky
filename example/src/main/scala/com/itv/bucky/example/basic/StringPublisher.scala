package com.itv.bucky.example.basic

import com.itv.lifecycle.Lifecycle
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/*
This example aims to give a minimal structure to:
 * Declare a queue, exchange and binding
 * Publish a raw String to a routing key
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringPublisher extends App {

  object Declarations {
    val queue      = Queue(QueueName("queue.string"))
    val routingKey = RoutingKey("stringPublisherRoutingKey")
    val exchange   = Exchange(ExchangeName("exchange.string-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val config                             = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key (along with exchange configuration) determine where the message will reach.
    */
  val publisherConfig =
    publishCommandBuilder(StringPayloadMarshaller) using Declarations.routingKey using Declarations.exchange.name

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Publisher[Future, String]] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _          <- DeclarationLifecycle(Declarations.all, amqpClient)
      publisher  <- amqpClient.publisherOf(publisherConfig)
    } yield publisher

  Lifecycle.using(lifecycle) { publisher: Publisher[Future, String] =>
    val result: Future[Unit] = publisher("Hello, world!")
    Await.result(result, 1.second)
  }

}
