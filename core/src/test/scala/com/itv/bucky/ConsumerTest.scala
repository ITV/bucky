package com.itv.bucky

import cats.effect.IO
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import com.itv.bucky.SuperTest.withDefaultClient
import com.itv.bucky.decl.{Exchange, Queue}

class ConsumerTest extends FunSuite with Eventually with IntegrationPatience with ScalaFutures {

  def accHandler = new Handler[IO, Delivery] {
    val acc = ListBuffer.empty[Delivery]
    override def apply(v1: Delivery): IO[ConsumeAction] = IO.delay {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
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

  test("should have a consumerOf method") {
    fail("do some work")
  }

}
