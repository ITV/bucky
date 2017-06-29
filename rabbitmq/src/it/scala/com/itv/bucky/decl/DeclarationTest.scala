package com.itv.bucky.decl

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky._
import com.itv.bucky.future.IntegrationUtils
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class DeclarationTest extends FunSuite with ScalaFutures with StrictLogging {
  import com.itv.bucky.future.FutureExt._

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

      val handler = new StubConsumeHandler[Future, Delivery]()
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

  test("Should be able to declare exchange and queue bindings") {
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

      val handler = new StubConsumeHandler[Future, Delivery]()
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

  test("Should be able to declare exchange and exchange bindings") {
    val destinationExchangeName = ExchangeName("bindingexdest-" + Random.nextInt())
    val sourceExchangeName = ExchangeName("bindingexsource-" + Random.nextInt())
    val exchangeBindingRoutingKey = RoutingKey("exbindingr" + Random.nextInt())

    val queueName = QueueName("bindingq" + Random.nextInt())

    Lifecycle.using(AmqpClientLifecycle(IntegrationUtils.config)) { amqpClient =>
      logger.info(s"AMQP Client started using config: ${IntegrationUtils.config}")
      val declarations = List(Exchange(destinationExchangeName,
        isDurable = false,
        shouldAutoDelete = true,
        isInternal = false,
        arguments = Map.empty),

        Exchange(sourceExchangeName,
          isDurable = false,
          shouldAutoDelete = true,
          isInternal = false,
          arguments = Map.empty),

        ExchangeBinding(destinationExchangeName,
          sourceExchangeName,
          exchangeBindingRoutingKey,
          Map.empty),

        Queue(queueName,
          isDurable = false,
          isExclusive = true,
          shouldAutoDelete = false,
          Map.empty),

        Binding(destinationExchangeName,
          queueName,
          exchangeBindingRoutingKey,
          Map.empty))

      logger.info(s"Declarations set as follows: $declarations")

      amqpClient.performOps(Declaration.runAll(declarations)) shouldBe 'success

      logger.info("Amqp run all declarations completed")

      val handler = new StubConsumeHandler[Future, Delivery]()
      val lifecycle =
        for {
          _ <- amqpClient.consumer(queueName, handler)
          _ = logger.info(s"Consumer set up on $queueName")
          serializer = publishCommandBuilder(StringPayloadMarshaller) using exchangeBindingRoutingKey using sourceExchangeName
          publisher <- amqpClient.publisher().map(AmqpClient.publisherOf(serializer))
          _ = logger.info(s"Publisher set up on $sourceExchangeName with $exchangeBindingRoutingKey")
        }
          yield publisher

      Lifecycle.using(lifecycle) { publisher =>
        logger.info("lifecycle started")
        handler.receivedMessages.size shouldBe 0
        publisher("Hello")
        logger.info("published hello message")
        eventually {
          handler.receivedMessages.size shouldBe 1
        }
      }
    }
  }
}
