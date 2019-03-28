package com.itv.bucky

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.scalatest.Matchers._

class PublisherTest extends FunSuite with ScalaFutures {
  def withDefaultClient(test: AmqpClient[IO] => IO[Unit]): Unit = {
    val ec                            = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    AmqpClient[IO](AmqpClientConfig("localhost", 5672, "guest", "guest"))
      .bracket(test)(_.shutdown())

  }
  test("A message can be published") {
    withDefaultClient { client =>
      val exchange = ExchangeName("test-exchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      client.publisher(10.second)(commandBuilder)
    }
  }

  test("A message should failt to be published on a non exitent exchange") {
    withDefaultClient { client =>
      val exchange = ExchangeName("non-existent-exchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      client
        .publisher(1.seconds)(commandBuilder)
        .attempt
        .map({
          _ shouldBe 'left
        })
    }
  }

  test("should have a publisherOf method") {
    fail("do some work")
  }
}
