package com.itv.bucky

import java.util.concurrent.TimeoutException

import cats.effect.IO
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import com.itv.bucky.publish._
import com.itv.bucky.consume._

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls
import com.itv.bucky.SuperTest.{StubChannel, withDefaultClient}
import com.itv.bucky.decl.{Exchange, Queue}

class PublishConsumeTest extends FunSuite with Eventually with IntegrationPatience with ScalaFutures {

  def accHandler = new Handler[IO, Delivery] {
    val acc: ListBuffer[Delivery] = ListBuffer.empty[Delivery]
    override def apply(v1: Delivery): IO[ConsumeAction] = IO.delay {
      acc.append(v1)
      Ack
    }
  }

  def stringAccHandler = new Handler[IO, String] {
    val acc: ListBuffer[String] = ListBuffer.empty
    override def apply(v1: String): IO[ConsumeAction] = IO.delay {
      acc.append(v1)
      Ack
    }
  }

  test("A message can be published and consumed") {
    withDefaultClient() { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val handler      = accHandler
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        _ <- client.publisher()(commandBuilder)
      } yield {
        handler.acc should have size 1
      }
    }
  }

  test("A message should fail publication if an ack is never returned") {
    withDefaultClient(publishTimeout = 2.seconds, channel = StubChannel.publishTimeout) { client =>
      val exchange = ExchangeName("anexchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey("ark")
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val handler      = accHandler
      val declarations = List(Queue(queue))
      for {
        _             <- client.declare(declarations)
        _             <- client.registerConsumer(queue, handler)
        publishResult <- client.publisher()(commandBuilder).attempt
      } yield {
        publishResult shouldBe 'left
        publishResult.left.get shouldBe a[TimeoutException]
        handler.acc should have size 0
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PublishCommandBuilder") {
    withDefaultClient() { client =>
      val exchange = ExchangeName("anexchange")
      val queue = QueueName("aqueue")
      val rk = RoutingKey("ark")
      val message = "Hello"
      implicit val commandBuilder: PublishCommandBuilder.Builder[String] =
        PublishCommandBuilder
          .publishCommandBuilder[String](StringPayloadMarshaller)
          .using(exchange)
          .using(rk)
      val handler = accHandler
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        publisher = client.publisherOf[String]
        _ <- publisher(message)
      } yield {
        handler.acc should have size 1
      }
    }
  }

  test("should have a publisherOf method that takes an implicit PayloadMarshaller") {
    withDefaultClient() { client =>
      val exchange = ExchangeName("anexchange")
      val queue = QueueName("aqueue")
      val rk = RoutingKey("ark")
      val message = "Hello"
      implicit val stringPayloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller

      val handler = accHandler
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queue, handler)
        publisher = client.publisherOf[String](exchange, rk)
        _ <- publisher(message)
      } yield {
        handler.acc should have size 1
      }
    }
  }

}
