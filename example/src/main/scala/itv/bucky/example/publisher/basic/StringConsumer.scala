package itv.bucky.example.publisher.basic

import com.itv.lifecycle.Lifecycle
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import itv.bucky._
import itv.bucky.decl.{DeclarationLifecycle, Queue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/*
This example aims to give a minimal structure to:
* Declare a queue
* Print any Strings to a stdout
It is not very useful by itself, but hopefully reveals the structure of how Bucky components fit together
 */

object StringConsumer extends App {

  object Declarations {
    val queue = Queue(QueueName("queue.string"))
    val all = List(queue)
  }

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  val stringToStdoutHandler =
    Handler { message: String =>
      Future.successful {
        println(message)
        Ack
      }
    }

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Unit] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      _ <- amqpClient.consumer(Declarations.queue.queueName, AmqpClient.handlerOf(stringToStdoutHandler, StringPayloadUnmarshaller))
    }
      yield ()

  lifecycle.runUntilJvmShutdown()

}
