package com.itv.bucky.test

import java.util.concurrent.TimeoutException

import cats.effect.IO
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.consume._
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.publish._
import com.itv.bucky.{ExchangeName, Handler, PayloadMarshaller, QueueName, RequeueHandler, RoutingKey}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import scala.language.reflectiveCalls

class PublishConsumeTest extends FunSuite with IOAmqpClientTest with Eventually with IntegrationPatience with ScalaFutures {

  test("A message can be published and consumed") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        _ <- client.publisher()(commandBuilder)
      } yield {
        handler.receivedMessages should have size 1
      }
    }
  }

  test("Can publish messages with headers") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)

      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      val headers: Map[String, AnyRef] = Map("foo" -> "bar")

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        publisher = new PublisherSugar(client).publisherWithHeadersOf(commandBuilder)
        _ <- publisher(message, headers)
      } yield {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head.properties.headers shouldBe headers
      }
    }
  }

  test("A message should fail publication if an ack is never returned") {
    runAmqpTestPublishTimeout { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue))
      for {
        _             <- client.declare(declarations)
        _             <- client.registerConsumer(queue, handler)
        publishResult <- client.publisher()(commandBuilder).attempt
      } yield {
        publishResult shouldBe 'left
        publishResult.left.get shouldBe a[TimeoutException]
        handler.receivedMessages should have size 0
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PublishCommandBuilder") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue = QueueName("aqueue")
      val rk = RoutingKey("ark")
      val message = "Hello"
      implicit val commandBuilder: PublishCommandBuilder.Builder[String] =
        PublishCommandBuilder
          .publishCommandBuilder[String](StringPayloadMarshaller)
          .using(exchange)
          .using(rk)
      val handler = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        publisher = client.publisherOf[String]
        _ <- publisher(message)
      } yield {
        handler.receivedMessages should have size 1
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PayloadMarshaller") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue = QueueName("aqueue")
      val rk = RoutingKey("ark")
      val message = "Hello"
      implicit val stringPayloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller

      val handler = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        publisher = client.publisherOf[String](exchange, rk)
        _ <- publisher(message)
      } yield {
        handler.receivedMessages should have size 1
      }
    }
  }

  test("should perform failure action when handler throws an exception") {
    runAmqpTest { client =>
      implicit val stringPayloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller

      val handler = new RequeueHandler[IO, String] {
        override def apply(v1: String): IO[RequeueConsumeAction] =
          IO {
            throw new RuntimeException("Oh no! it happened again")
          }
      }
      val exchange = ExchangeName("anexchange")

      val queue = QueueName("aqueue")
      val rk = RoutingKey("ark")

      val declarations =
        List(Queue(queue), Exchange(exchange).binding(rk -> queue)) ++ com.itv.bucky.pattern.requeue.requeueDeclarations(queue, rk)

      val requeueHandler = StubHandlers.ackHandler[IO, String]

      for {
        _ <- client.declare(declarations)
        _ <- client.registerRequeueConsumerOf(queue, handler)
        publisher = client.publisherOf[String](exchange, rk)
        _ <- client.registerConsumerOf(QueueName(queue.value + ".requeue"), requeueHandler)
        _ <- publisher("hello")
      }
        yield {
          requeueHandler.receivedMessages should have size 1
        }
    }
  }

}
