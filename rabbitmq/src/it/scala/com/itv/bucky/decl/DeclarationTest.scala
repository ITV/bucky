package com.itv.bucky.decl

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky.SameThreadExecutionContext.implicitly
import com.itv.bucky._
import com.itv.lifecycle.Lifecycle
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random

class DeclarationTest extends FunSuite with ScalaFutures {

  implicit val declarationPatienceConfig: Eventually.PatienceConfig =
    Eventually.PatienceConfig(timeout = 5.second, interval = 100.millis)

  test("Should be able to declare a queue") {
    val queueName = "queue.declare" + Random.nextInt()

    Lifecycle.using(AmqpClientLifecycle(IntegrationUtils.config)) { amqpClient =>
      amqpClient.performOps(Queue(QueueName(queueName),
        isDurable = true,
        isExclusive = true,
        shouldAutoDelete = false,
        Map.empty).run) shouldBe 'success

      val handler = new StubConsumeHandler[Delivery]()
      val lifecycle =
        for {
          _ <- amqpClient.consumer(QueueName(queueName), handler)
          serializer = publishCommandBuilder(StringPayloadMarshaller) using RoutingKey(queueName) using ExchangeName("")
          publisher <- amqpClient.publisher().map(AmqpClient.publisherOf(serializer))
        }
          yield publisher

      Lifecycle.using(lifecycle) { publisher =>
        handler.receivedMessages.size shouldBe 0
        publisher("Hello")
        eventually {
          handler.receivedMessages.size shouldBe 1
        }
      }
    }
  }

  test("Should be able to declare exchange and bindings") {
    val exchangeName = ExchangeName("bindingex-" + Random.nextInt())
    val queueName = QueueName("bindingq" + Random.nextInt())
    val routingKey = RoutingKey("bindingr" + Random.nextInt())

    Lifecycle.using(AmqpClientLifecycle(IntegrationUtils.config)) { amqpClient =>
      val declarations = List(Exchange(exchangeName,
        isDurable = false,
        shouldAutoDelete = true,
        isInternal = false,
        arguments = Map.empty),

      Queue(queueName,
        isDurable = false,
        isExclusive = true,
        shouldAutoDelete = false,
        Map.empty),

      Binding(exchangeName,
        queueName,
        routingKey,
        Map.empty))

      amqpClient.performOps(Declaration.runAll(declarations)) shouldBe 'success

      val handler = new StubConsumeHandler[Delivery]()
      val lifecycle =
        for {
          _ <- amqpClient.consumer(queueName, handler)
          serializer = publishCommandBuilder(StringPayloadMarshaller) using routingKey using exchangeName
          publisher <- amqpClient.publisher().map(AmqpClient.publisherOf(serializer))
        }
          yield publisher

      Lifecycle.using(lifecycle) { publisher =>
        handler.receivedMessages.size shouldBe 0
        publisher("Hello")
        eventually {
          handler.receivedMessages.size shouldBe 1
        }
      }
    }

  }

}
