package com.itv.bucky.test

import cats.effect.{IO, Resource}
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.consume.{Ack, DeadLetter}
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.{ExchangeName, QueueName, RoutingKey, consume, _}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class StubTest extends AnyFunSuite with IOAmqpClientTest {
  val exchange     = ExchangeName("anexchange")
  val queue        = QueueName("aqueue")
  val rk           = RoutingKey("ark")
  val message      = "Hello"
  val exception    = new RuntimeException("expected")
  val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

  test("An ack Recording Handlers should accumulate publish results and ack") {
    runAmqpTestAllAck { client =>
      val consumer = StubHandlers.ackHandler[IO, String]
      Resource.eval(client.declare(declarations)).flatMap(_ => client.registerConsumerOf(queue, consumer)).use { _ =>
        for {
          publisher <- IO(client.publisherOf[String](exchange, rk))
          _         <- (1 to 10).toList.map(_ => publisher(message)).sequence
        } yield {
          all(consumer.receivedMessages) should be(message)
          all(consumer.returnedResults) should be(Right(Ack))
        }
      }
    }
  }

  test("Should not suffer from deadlock") {
    runAmqpTestAllAck { client =>
      val publisher = client.publisherOf[String](ExchangeName("x"), RoutingKey("y"))
      val handler = new Handler[IO, String] {
        override def apply(delivery: String): IO[consume.ConsumeAction] =
          publisher("publish from handler").map(_ => Ack)
      }

      Resource
        .eval(client.declare(declarations))
        .flatMap { _ =>
          client.registerConsumerOf(queue, handler)
        }
        .use { _ =>
          val publisher = client.publisherOf[String](exchange, rk)
          publisher(message)
        }
    }
  }

  test("Stub publisher should capture messages") {
    runAmqpTestAllAck { client =>
      val stubPubslisher = StubPublishers.stubPublisher[IO, String]
      val handler = new Handler[IO, String] {
        override def apply(delivery: String): IO[consume.ConsumeAction] =
          stubPubslisher(delivery).map(_ => Ack)
      }
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, handler)
      } yield ())
        .use { _ =>
          for {
            publisher <- IO(client.publisherOf[String](exchange, rk))
            _         <- publisher(message)
          } yield stubPubslisher.recordedMessages shouldBe List(message)
        }
    }
  }

  test("Multiple Recording Handlers can be registered") {
    val consumer1 = StubHandlers.ackHandler[IO, String]
    val consumer2 = StubHandlers.ackHandler[IO, String]
    val queue2    = QueueName("queue2")
    runAmqpTestAllAck { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations ++ List(Queue(queue2), Exchange(exchange).binding(rk -> queue2))))
        _ <- client.registerConsumerOf(queue, consumer1)
        _ <- client.registerConsumerOf(queue2, consumer2)
      } yield ()).use { _ =>
        for {
          publisher <- IO(client.publisherOf[String](exchange, rk))
          _         <- (1 to 10).toList.map(_ => publisher(message)).sequence
        } yield {
          all(consumer1.receivedMessages) should be(message)
          all(consumer1.returnedResults) should be(Right(Ack))
          all(consumer2.receivedMessages) should be(message)
          all(consumer2.returnedResults) should be(Right(Ack))
        }
      }
    }
  }

  test("An all shall ack client should fail to publish if a handler doesn't return an Ack") {
    val consumer = StubHandlers.deadLetterHandler[IO, String]
    runAmqpTestAllAck { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)
      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          publishRes shouldBe Symbol("left")
        }

      }
    }
  }

  test("An all shall ack client should fail to publish if a handler returns an exception") {
    val consumer = StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception))
    runAmqpTestAllAck { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)
      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          consumer.returnedResults shouldBe List(Left(exception))
          publishRes shouldBe Symbol("left")
        }
      }
    }
  }

  test("Forgiving client should not fail to publish if a handler doesn't return an Ack") {
    val consumer = StubHandlers.deadLetterHandler[IO, String]
    runAmqpTestForgiving { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)
      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          publishRes shouldBe Symbol("right")
        }
      }
    }
  }

  test("Forgiving client should not fail to publish if a handler returns an exception") {
    val consumer = StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception))
    runAmqpTestForgiving { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)

      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          publishRes shouldBe Symbol("right")
        }
      }
    }
  }

  test("An strict simulator client should not fail publish if a handler doesn't return an Ack") {
    val consumer = StubHandlers.deadLetterHandler[IO, String]
    runAmqpTestStrict { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)
      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          consumer.returnedResults shouldBe List(Right(DeadLetter))
          publishRes shouldBe Symbol("right")
        }
      }
    }
  }

  test("A strict simulator client should fail to publish if a handler returns an exception") {
    val consumer = StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception))
    runAmqpTestStrict { client =>
      (for {
        _ <- Resource.eval(client.declare(declarations))
        _ <- client.registerConsumerOf(queue, consumer)

      } yield ()).use { _ =>
        for {
          publisher  <- IO(client.publisherOf[String](exchange, rk))
          publishRes <- publisher(message).attempt
        } yield {
          consumer.receivedMessages shouldBe List(message)
          consumer.returnedResults shouldBe List(Left(exception))
          publishRes shouldBe Symbol("left")
        }
      }

    }
  }

}
