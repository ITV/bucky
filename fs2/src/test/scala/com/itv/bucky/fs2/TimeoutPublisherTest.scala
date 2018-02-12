package com.itv.bucky.fs2

import java.util.concurrent.TimeoutException

import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.bucky._
import com.rabbitmq.client.AMQP
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class TimeoutPublisherTest extends FunSuite with ScalaFutures with StrictLogging {
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  test("Returns timeout of delegate publisher if result occurs after timeout") {
    val client = IOAmqpClient(new StubChannel {
      override def deliver(delivery: AMQP.Basic.Deliver, body: Payload, properties: AMQP.BasicProperties): Unit =
        while (true) {}
    })

    val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))
    val builder                             = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey("")
    client
      .publisherOf[Unit](builder, 10.milliseconds)
      .apply(())
      .attempt
      .unsafeRunSync()
      .fold(
        exception => {
          exception shouldBe a[TimeoutException]
          exception.getMessage should include("Timed out").and(include("10 milliseconds"))
        },
        _ => fail("It should not publish the message")
      )
  }
}
