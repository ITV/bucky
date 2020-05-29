package com.itv.bucky.test

import java.util.concurrent.TimeoutException

import cats.effect.{IO, Resource}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.consume._
import com.itv.bucky.decl.{Direct, Exchange, ExchangeBinding, Headers, Queue, Topic}
import com.itv.bucky.publish._
import com.itv.bucky.{ExchangeName, Handler, PayloadMarshaller, PublisherSugar, QueueName, RequeueHandler, RoutingKey}
import org.scalatest.{EitherValues, FunSuite}
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.language.reflectiveCalls

class PublishConsumeTest extends AnyFunSuite with IOAmqpClientTest with Eventually with IntegrationPatience with ScalaFutures with EitherValues {

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

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder)
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("A message can be published to a Topic exchange and consumed from a queue bound with wildcard routing key") {
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
      val declarations = List(Queue(queue), Exchange(exchange, exchangeType = Topic).binding((RoutingKey("#"), queue)))

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder)
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("A message can be published to a Topic exchange and consumed from a queue bound with wildcard suffix routing key" +
    "which matches more than 0 words") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rkRouted       = RoutingKey("ar.k")
      val rkUnrouted       = RoutingKey("ask")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)

      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange, exchangeType = Topic).binding((RoutingKey("ar.#"), queue)))

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder.using(rkRouted).toPublishCommand(message))
          _ <- client.publisher()(commandBuilder.using(rkUnrouted).toPublishCommand(message))
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("A message can be published to a Topic exchange and consumed from a queue bound with wildcard suffix routing key" +
    "which matches 0 words") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rkRouted       = RoutingKey("ar")
      val rkUnrouted       = RoutingKey("ask")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)

      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange, exchangeType = Topic).binding((RoutingKey("ar.#"), queue)))

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder.using(rkRouted).toPublishCommand(message))
          _ <- client.publisher()(commandBuilder.using(rkUnrouted).toPublishCommand(message))
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("Can simulate exchange bindings") {
    runAmqpTest { client =>
      val exchangeA = ExchangeName("a")
      val exchangeB = ExchangeName("b")
      val queue     = QueueName("aqueue")
      val rk        = RoutingKey("ark")
      val message   = "Hello"

      val declarations = List(
        Queue(queue),
        Exchange(exchangeA),
        Exchange(exchangeB).binding(rk -> queue),
        ExchangeBinding(exchangeB, exchangeA, rk)
      )

      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchangeA)
        .using(rk)
        .toPublishCommand(message)
      val handler = StubHandlers.ackHandler[IO, Delivery]

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder)
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("Can simulate headers bindings (match any headers)") {
    runAmqpTest { client =>
      val exchange = ExchangeName("ex")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val otherRk  = RoutingKey("anrk")
      val message  = "Hello"
      val header   = "key" -> "val"

      val declarations = List(
        Queue(queue),
        Exchange(exchange, Headers).binding(rk -> queue, Map("x-match" -> "any", header))
      )

      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(otherRk)
        .using(MessageProperties.minimalBasic.withHeader(header))
        .toPublishCommand(message)

      val handler = StubHandlers.ackHandler[IO, Delivery]

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder)
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("Can simulate headers bindings (match ALL headers)") {
    runAmqpTest { client =>
      val exchange = ExchangeName("ex")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val otherRk  = RoutingKey("anrk")
      val message  = "Hello"
      val header   = "key" -> "val"
      val header2  = "key2" -> "val2"

      val declarations = List(
        Queue(queue),
        Exchange(exchange, Headers).binding(rk -> queue, Map("x-match" -> "all", header, header2))
      )

      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(otherRk)

      val message1 = commandBuilder
        .using(MessageProperties.minimalBasic.withHeader(header))
        .toPublishCommand(message)

      val message2 = commandBuilder
        .using(MessageProperties.minimalBasic.withHeader(header).withHeader(header2))
        .toPublishCommand(message)

      val handler = StubHandlers.ackHandler[IO, Delivery]

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(message1)
          firstCount = handler.receivedMessages.size
          _ <- client.publisher()(message2)
          secondCount = handler.receivedMessages.size
        } yield {
          firstCount shouldBe 0
          secondCount shouldBe 1
        }
      }
    }
  }

  test("Can simulate exchange binding with a Headers destination exchange") {
    runAmqpTest { client =>
      val exchangeA = ExchangeName("a")
      val exchangeB = ExchangeName("b")
      val queue     = QueueName("aqueue")
      val rk        = RoutingKey("ark")
      val otherRk   = RoutingKey("anrk")
      val message   = "Hello"
      val header    = "key" -> "val"

      val declarations = List(
        Queue(queue),
        Exchange(exchangeA),
        Exchange(exchangeB, Headers).binding(rk -> queue, Map("x-match" -> "any", header)),
        ExchangeBinding(exchangeB, exchangeA, otherRk)
      )

      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchangeA)
        .using(otherRk)
        .using(MessageProperties.minimalBasic.withHeader(header))
        .toPublishCommand(message)

      val handler = StubHandlers.ackHandler[IO, Delivery]

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          _ <- client.publisher()(commandBuilder)
        } yield {
          handler.receivedMessages should have size 1
        }
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

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        val publisher = new PublisherSugar(client).publisherWithHeadersOf(commandBuilder)
        for {
          _ <- publisher(message, headers)
        } yield {
          handler.receivedMessages should have size 1
          handler.receivedMessages.head.properties.headers shouldBe headers
        }
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

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        for {
          publishResult <- client.publisher()(commandBuilder).attempt
        } yield {
          publishResult.left.value shouldBe a[TimeoutException]
          handler.receivedMessages should have size 0
        }
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PublishCommandBuilder") {
    runAmqpTest { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      implicit val commandBuilder: PublishCommandBuilder.Builder[String] =
        PublishCommandBuilder
          .publishCommandBuilder[String](StringPayloadMarshaller)
          .using(exchange)
          .using(rk)
      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        val publisher = client.publisherOf[String]
        for {
          _ <- publisher(message)
        } yield {
          handler.receivedMessages should have size 1
        }
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PayloadMarshaller") {
    runAmqpTest { client =>
      val exchange                                                    = ExchangeName("anexchange")
      val queue                                                       = QueueName("aqueue")
      val rk                                                          = RoutingKey("ark")
      val message                                                     = "Hello"
      implicit val stringPayloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller

      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      Resource.liftF(client.declare(declarations)).flatMap(_ => client.registerConsumer(queue, handler)).use { _ =>
        val publisher = client.publisherOf[String](exchange, rk)
        for {
          _ <- publisher(message)
        } yield {
          handler.receivedMessages should have size 1
        }
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
      val rk    = RoutingKey("ark")

      val declarations =
        List(Queue(queue), Exchange(exchange).binding(rk -> queue)) ++ com.itv.bucky.pattern.requeue.requeueDeclarations(queue)

      val requeueHandler = StubHandlers.ackHandler[IO, String]

      Resource
        .liftF(client.declare(declarations))
        .flatMap(_ =>
          for {
            _ <- client.registerRequeueConsumerOf(queue, handler)
            _ <- client.registerConsumerOf(QueueName(queue.value + ".requeue"), requeueHandler)
          } yield ())
        .use { _ =>
          val publisher = client.publisherOf[String](exchange, rk)
          for {
            _ <- publisher("hello")
          } yield {
            requeueHandler.receivedMessages should have size 1
          }
        }
    }
  }

}
