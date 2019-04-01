package com.itv.bucky

import java.util.UUID

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl.{Declaration, Exchange, Queue}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import com.itv.bucky.SuperTest.withDefaultClient

class DeclarationTest extends FunSuite with Eventually with IntegrationPatience with ScalaFutures {

  def accHandler = new Handler[IO, Delivery] {
    val acc = ListBuffer.empty[Delivery]
    override def apply(v1: Delivery): IO[ConsumeAction] = IO.delay {
      acc.append(v1)
      Ack
    }
  }

  test("We can declare things") {
    withDefaultClient { client =>
      val exchangeName = ExchangeName(UUID.randomUUID().toString)
      val queueName    = QueueName(UUID.randomUUID().toString)
      val rk       = RoutingKey(UUID.randomUUID().toString)
      val message  = "Hello"

      val declarations = List(
        Queue(name = queueName),
        Exchange(name = exchangeName)
          .binding(rk -> queueName)
      )

      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchangeName)
        .using(rk)
        .toPublishCommand(message)

      val handler = accHandler
      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumer(queueName, handler.apply)
        _ <- client.publisher()(commandBuilder)
      } yield {
        eventually {
          handler.acc should have size 1
        }
      }
    }
  }

}
