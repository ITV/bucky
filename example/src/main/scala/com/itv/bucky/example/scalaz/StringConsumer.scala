package com.itv.bucky.example.scalaz

import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.taskz.ProcessAmqpClient
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scalaz.concurrent.Task

/*
This example aims to give a minimal structure to:
* Declare a queue
* Print any Strings to a stdout
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringConsumer extends App with StrictLogging {

  object Declarations {
    val queue = Queue(QueueName("queue.string"))
    val all = List(queue)
  }

  val config = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  val stringToLogHandler =
    Handler[Task, String] { message: String =>
      Task {
        logger.info(message)
        Ack
      }
    }


  /***
    * Create the process amqpClient to consume messages from a queue
    */
  val consumerProcess = ProcessAmqpClient.fromConfig(amqpClientConfig) { amqpClient =>
    DeclarationExecutor(Declarations.all, amqpClient)
    amqpClient.consumer(Declarations.queue.name, AmqpClient.handlerOf(stringToLogHandler, StringPayloadUnmarshaller)(amqpClient.F))
  }

  consumerProcess.run.unsafePerformSync
}
