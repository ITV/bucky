package com.itv.bucky

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PublisherTest extends FunSuite with ScalaFutures {
  def withDefaultClient(test: AmqpClient[IO] => IO[Unit]): Unit = {
    val ec                            = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    AmqpClient[IO](AmqpClientConfig("localhost", 5672, "guest", "guest"))
      .flatMap(test)
      .map(_ => Thread.sleep(40000))
      .unsafeRunSync()
  }
  test("A message can be published") {
    withDefaultClient { client =>
      val exchange = ExchangeName("dasdasd")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val result = client.publisher(10.seconds)(commandBuilder)
      result
    }
  }
}
