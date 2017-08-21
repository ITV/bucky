package com.itv.bucky.example.basic

import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.itv.bucky._
import com.itv.lifecycle.Lifecycle
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.{Await, Future}

object RawConsumer extends App with StrictLogging {

  //start snippet 1
  val brokerHostname   = ConfigFactory.load("bucky").getString("rmq.host")
  val amqpClientConfig = AmqpClientConfig(brokerHostname, 5672, "guest", "guest")

  object Declarations {
    val queue      = Queue(QueueName("raw.consumer.queue"))
    val routingKey = RoutingKey("raw.publish.routingkey")
    val exchange =
      Exchange(ExchangeName("raw.publisher.exchange"))
        .binding(routingKey -> queue.name)
    val all = Seq(queue, exchange)
  }

  case class Person(name: String, age: Int)

  //end snippet 1

  //start snippet 2
  val handler = Handler[Future, Delivery] { delivery =>
    Future {
      //use a PayloadUnmarshaller[String] to unmarshal the Payload
      delivery.body.unmarshal[String] match {
        //if it was successfully translated into a UTF8 string
        //log and acknowledge the message (remove from queue)
        case UnmarshalResult.Success(string) =>
          logger.info("Received message " + string)
          Ack
        //if we CANNOT translate this payload into into a UTF8 string
        //log and deadletter the message
        //(remove from queue, re-route to the deadletter exchange configured
        //on the queue
        case UnmarshalResult.Failure(reason, _) =>
          logger.error(s"could not translate message to a string")
          DeadLetter
      }
    }
  }
  //end snippet 2

  //start snippet 3
  val consumerLifecycle: Lifecycle[Unit] =
    for {
      //create a connection to the broker
      client <- AmqpClientLifecycle(amqpClientConfig)
      //declare the exchange we will publish to
      _ <- DeclarationLifecycle(Declarations.all, client)
      //create an instance of a generic publisher
      _ <- client.consumer(Declarations.queue.name, handler)
    } yield ()

  consumerLifecycle.runUntilJvmShutdown()
  //end snippet 3

}
