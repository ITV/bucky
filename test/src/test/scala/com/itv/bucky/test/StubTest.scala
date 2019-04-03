package com.itv.bucky.test

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.{AmqpClient, ExchangeName, QueueName, RoutingKey, consume}
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.publish.PublishCommandBuilder

import scala.concurrent.ExecutionContext

class StubTest extends FunSuite with Matchers {
  val exchange = ExchangeName("anexchange")
  val queue    = QueueName("aqueue")
  val rk       = RoutingKey("ark")
  val message  = "Hello"
  val commandBuilder: consume.PublishCommand = PublishCommandBuilder
    .publishCommandBuilder[String](StringPayloadMarshaller)
    .using(exchange)
    .using(rk)
    .toPublishCommand(message)
  val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

  def withAllAckClient(test: AmqpClient[IO] => IO[Unit])(implicit ec: ExecutionContext = ExecutionContext.global) = {
    val config                        = Config.empty(3.seconds)
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    TestAmqpClient.allShallAckSimulator[IO](config).flatMap(test).unsafeRunSync()
  }

  test("An ack stub handler should accumulate publish results and ack") {
    withAllAckClient { client =>
      for {
        _         <- client.declare(declarations)
        consumer  <- IO.pure(Consumer.ackConsumer[IO, String])
        _         <- client.registerConsumer(queue, consumer)
        publisher <- IO(client.publisher())
        _         <- (1 to 10).toList.map(_ => publisher(commandBuilder)).sequence
      } yield {
        consumer.messagesReceived shouldBe (1 to 10).map(message).toList
      }
    }
  }
}
