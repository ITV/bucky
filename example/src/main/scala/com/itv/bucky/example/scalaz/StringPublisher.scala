package com.itv.bucky.example.scalaz

import java.util.Date

import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.taskz.TaskAmqpClient
import com.typesafe.config.ConfigFactory

/*
This example aims to give a minimal structure, using TaskAmqpClient, to:
* Declare a queue, exchange and binding
* Publish a raw String to a routing key
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringPublisher extends App {

  object Declarations {
    val queue = Queue(QueueName("queue.string"))
    val routingKey = RoutingKey("stringPublisherRoutingKey")
    val exchange = Exchange(ExchangeName("exchange.string-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val config = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key (along with exchange configuration) determine where the message will reach.
    */
  val publisherConfig =
    publishCommandBuilder(StringPayloadMarshaller) using Declarations.routingKey using Declarations.exchange.name


  /** *
    * Create the task amqpClient with id and create a task to shutdown the client
    */

  val amqpClient = TaskAmqpClient.fromConfig(amqpClientConfig)
  val shutdown = TaskAmqpClient.closeAll(amqpClient)

  DeclarationExecutor(Declarations.all, amqpClient)
  val publish = amqpClient.publisherOf(publisherConfig)
  val task = publish(s"Hello, world at ${new Date()}!")

  val foo = task.unsafePerformSync

  shutdown.unsafePerformSync
}
