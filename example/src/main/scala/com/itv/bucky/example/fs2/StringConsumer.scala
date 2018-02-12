package com.itv.bucky.example.fs2

import cats.effect.IO
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.fs2.IOAmqpClient
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import _root_.fs2.Stream
import com.itv.bucky.future.SameThreadExecutionContext

/*
This example aims to give a minimal structure to:
 * Declare a queue
 * Print any Strings to a stdout
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringConsumer extends App with StrictLogging {
  import SameThreadExecutionContext.implicitly
  import fs2.ioMonadError

  object Declarations {
    val queue = Queue(QueueName("queue.string"))
    val all   = List(queue)
  }

  val config           = ConfigFactory.load("bucky")
  val amqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  val stringToLogHandler =
    Handler[IO, String] { message: String =>
      IO {
        logger.info(message)
        Ack
      }
    }

  IOAmqpClient
    .use(amqpClientConfig, Declarations.all) { amqpClient =>
      amqpClient.consumer(Declarations.queue.name, AmqpClient.handlerOf(stringToLogHandler, StringPayloadUnmarshaller))
    }
    .compile
    .drain
    .unsafeRunSync()
}
