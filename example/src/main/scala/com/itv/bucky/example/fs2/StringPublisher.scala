package com.itv.bucky.example.fs2

import java.util.Date

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.fs2._
import com.typesafe.config.ConfigFactory
import _root_.fs2.Stream
import com.itv.bucky.future.SameThreadExecutionContext

/*
This example aims to give a minimal structure, using IOAmqpClient, to:
 * Declare a queue, exchange and binding
 * Publish a raw String to a routing key
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringPublisher extends App {
  import SameThreadExecutionContext.implicitly

  object Declarations {
    val queue      = Queue(QueueName("queue.string"))
    val routingKey = RoutingKey("stringPublisherRoutingKey")
    val exchange   = Exchange(ExchangeName("exchange.string-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val config           = ConfigFactory.load("bucky")
  val amqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key (along with exchange configuration) determine where the message will reach.
    */
  val publisherConfig =
    publishCommandBuilder(StringPayloadMarshaller) using Declarations.routingKey using Declarations.exchange.name

  /** *
    * Create the task amqpClient with id and create a task to shutdown the client
    */
  val p = for {
    amqpClient <- clientFrom(amqpClientConfig, Declarations.all)
    publish = amqpClient.publisherOf(publisherConfig)
    _ <- Stream.eval(publish(s"Hello, world at ${new Date()}!"))
  } yield ()

  p.compile.last
    .unsafeRunSync()

}
