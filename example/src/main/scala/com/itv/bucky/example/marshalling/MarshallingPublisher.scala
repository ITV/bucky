package com.itv.bucky.example.marshalling

import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.Lifecycle
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.future._
import com.itv.bucky.example.marshalling.Shared.Person

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._


object MarshallingPublisher extends App {

  object Declarations {
    val queue = Queue(QueueName("queue.people"))
    val routingKey = RoutingKey("personPublisherRoutingKey")
    val exchange = Exchange(ExchangeName("exchange.person-publisher")).binding(routingKey -> queue.name)

    val all = List(queue, exchange)
  }

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key, along with exchange name, determine where the message will reach.
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
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      publisher <- amqpClient.publisherOf(publisherConfig)
    }
      yield publisher

  Lifecycle.using(lifecycle) { publisher: Publisher[Future, Person] =>
    val result: Future[Unit] = publisher(Person("Bob", 21))
    Await.result(result, 1.second)
  }

}
