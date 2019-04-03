package com.itv.bucky.test

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.{AmqpClient, ExchangeName, QueueName, RoutingKey, consume}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.publish.PublishCommandBuilder
import com.itv.bucky._
import com.itv.bucky.consume.{Ack, DeadLetter}

import scala.concurrent.ExecutionContext

class StubTest extends FunSuite with Matchers {
  val exchange     = ExchangeName("anexchange")
  val queue        = QueueName("aqueue")
  val rk           = RoutingKey("ark")
  val message      = "Hello"
  val exception    = new RuntimeException("expected")
  val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

  def withAllAckClient(test: AmqpClient[IO] => IO[Unit])(implicit ec: ExecutionContext = ExecutionContext.global) = {
    val config                        = Config.empty(3.seconds)
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    TestAmqpClient.allShallAckSimulator[IO](config).use(test).unsafeRunSync()
  }

  def withForgivingClient(test: AmqpClient[IO] => IO[Unit])(implicit ec: ExecutionContext = ExecutionContext.global) = {
    val config                        = Config.empty(3.seconds)
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    TestAmqpClient.forgivingSimulator[IO](config).use(test).unsafeRunSync()
  }

  def withStrictSimulator(test: AmqpClient[IO] => IO[Unit])(implicit ec: ExecutionContext = ExecutionContext.global) = {
    val config                        = Config.empty(3.seconds)
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    TestAmqpClient.strictSimulator[IO](config).use(test).unsafeRunSync()
  }

  test("An ack Recording Handlers should accumulate publish results and ack") {
    withAllAckClient { client =>
      for {
        _         <- client.declare(declarations)
        consumer  <- IO.pure(StubHandlers.ackHandler[IO, String])
        _         <- client.registerConsumerOf(queue, consumer)
        publisher <- IO(client.publisherOf[String](exchange, rk))
        _         <- (1 to 10).toList.map(_ => publisher(message)).sequence
      } yield {
        all(consumer.receivedMessages) should be(message)
        all(consumer.returnedResults) should be(Right(Ack))
      }
    }
  }

  test("Multiple Recording Handlers can be registered") {
    withAllAckClient { client =>
      val queue2 = QueueName("queue2")
      for {
        _         <- client.declare(declarations ++ List(Queue(queue2), Exchange(exchange).binding(rk -> queue2)))
        consumer1 <- IO.pure(StubHandlers.ackHandler[IO, String])
        consumer2 <- IO.pure(StubHandlers.ackHandler[IO, String])
        _         <- client.registerConsumerOf(queue, consumer1)
        _         <- client.registerConsumerOf(queue2, consumer2)
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

  test("An all shall ack client should fail to publish if a handler doesn't return an Ack") {
    withAllAckClient { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.deadLetterHandler[IO, String])
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        publishRes shouldBe 'left
      }
    }
  }

  test("An all shall ack client should fail to publish if a handler returns an exception") {
    withAllAckClient { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception)))
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        consumer.returnedResults shouldBe List(Left(exception))
        publishRes shouldBe 'left
      }
    }
  }

  test("Forgiving client should not fail to publish if a handler doesn't return an Ack") {
    withForgivingClient { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.deadLetterHandler[IO, String])
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        publishRes shouldBe 'right
      }
    }
  }

  test("Forgiving client should not fail to publish if a handler returns an exception") {
    withForgivingClient { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception)))
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        publishRes shouldBe 'right
      }
    }
  }

  test("An strict simulator client should not fail publish if a handler doesn't return an Ack") {
    withStrictSimulator { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.deadLetterHandler[IO, String])
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        consumer.returnedResults shouldBe List(Right(DeadLetter))
        publishRes shouldBe 'right
      }
    }
  }

  test("A strict simulator client should fail to publish if a handler returns an exception") {
    withStrictSimulator { client =>
      for {
        _          <- client.declare(declarations)
        consumer   <- IO.pure(StubHandlers.recordingHandler[IO, String](_ => IO.raiseError(exception)))
        _          <- client.registerConsumerOf(queue, consumer)
        publisher  <- IO(client.publisherOf[String](exchange, rk))
        publishRes <- publisher(message).attempt
      } yield {
        consumer.receivedMessages shouldBe List(message)
        consumer.returnedResults shouldBe List(Left(exception))
        publishRes shouldBe 'left
      }
    }
  }

}
