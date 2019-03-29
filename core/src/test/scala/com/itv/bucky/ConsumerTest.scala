package com.itv.bucky

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.reflectiveCalls

class ConsumerTest extends FunSuite with Eventually with IntegrationPatience with ScalaFutures {
  def withDefaultClient(test: AmqpClient[IO] => IO[Unit]): Unit = {
    val ec                            = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    AmqpClient[IO](AmqpClientConfig("172.17.0.2", 5672, "guest", "guest"))
      .bracket(test)(_.shutdown())
      .unsafeRunSync()
  }

  def accHandler = new Handler[IO, Delivery] {
    val acc = ListBuffer.empty[Delivery]
    override def apply(v1: Delivery): IO[ConsumeAction] = IO.delay {
      acc.append(v1)
      Ack
    }
  }

  test("A message can be published and consumed") {
    withDefaultClient { client =>
      val exchange = ExchangeName("")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val handler = accHandler
      for {
        _ <- client.registerConsumer(queue, handler.apply)
        _ <- client.publisher(10.second)(commandBuilder)
      } yield {
        eventually {
          handler.acc should have size 1
        }
      }
    }
  }

  test("should have a consumerOf method") {
    fail("do some work")
  }

}
