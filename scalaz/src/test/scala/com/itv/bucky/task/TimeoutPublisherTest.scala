package com.itv.bucky.task

import java.util.concurrent.TimeoutException

import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.bucky._
import com.rabbitmq.client.AMQP
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scalaz.{-\/, \/-}
import scala.concurrent.duration._

class TimeoutPublisherTest extends FunSuite with ScalaFutures with StrictLogging {

  test("Returns timeout of delegate publisher if result occurs after timeout") {
    val client = TaskAmqpClient(new StubChannel {
      override def deliver(delivery: AMQP.Basic.Deliver, body: Payload, properties: AMQP.BasicProperties): Unit = {
        while (true) {
        }
      }
    })

    val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))
    val builder = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey("")
    client.publisherOf[Unit](builder, 10.milliseconds).apply(()).unsafePerformSyncAttempt match {
      case -\/(exception) =>
        exception shouldBe a[TimeoutException]
        exception.getMessage should include ("Timed out").and(include("10 milliseconds"))
      case \/-(()) => fail("It should not publish the message")
    }
  }
}
