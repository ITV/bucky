package itv.bucky.example.requeue

import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import itv.bucky._
import itv.bucky.decl.{DeclarationLifecycle, Queue}
import itv.bucky.pattern.requeue._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object RequeueConsumer extends App with StrictLogging {

  object Declarations {
    val queue = Queue(QueueName("requeue.string"))
    val all = List(queue) ++ basicRequeueDeclarations(queue.queueName)
  }

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  val stringToLogRequeueHandler =
    RequeueHandler { message: String =>
      Future {
        logger.info(message)

        message match {
          case "requeue" => Requeue
          case "deadletter" => DeadLetter
          case _ => Ack
        }
      }
    }

  val requeuePolicy = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 10.seconds)

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Unit] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      _ <- amqpClient.requeueHandlerOf(Declarations.queue.queueName, stringToLogRequeueHandler, requeuePolicy, StringPayloadUnmarshaller)
    }
      yield ()

  lifecycle.runUntilJvmShutdown()

}
