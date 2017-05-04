package com.itv.bucky

import com.itv.bucky.SameThreadExecutionContext.implicitly
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}
import com.rabbitmq.client.impl.AMQImpl.Basic
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.concurrent.Future

class ConsumerTest extends FunSuite {

  test("Runs callback with delivered messages") {
    val channel = new StubChannel()
    val client = createClient(channel)

    val handler = new StubConsumeHandler[Delivery]()

    Lifecycle.using(client.consumer(QueueName("blah"), handler, prefetchCount = 12)) { _ =>

      channel.consumers should have size 1
      channel.setPrefetchCount shouldBe 12

      val msg = Payload.from("Hello World!")

      handler.nextResponse = Future.successful(Ack)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      channel.transmittedCommands.last shouldBe a[Basic.Ack]

      handler.nextResponse = Future.successful(DeadLetter)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      channel.transmittedCommands.last shouldBe a[Basic.Nack]
      channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false

      handler.nextResponse = Future.successful(RequeueImmediately)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      channel.transmittedCommands.last shouldBe a[Basic.Nack]
      channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe true

      handler.receivedMessages should have size 3
      1 shouldEqual 1
    }
  }

  test("Should send exceptionalAction when an exception occurs in the handler") {
    val channel = new StubChannel()
    val client = createClient(channel)

    val handler = new StubConsumeHandler[Delivery]()

    Lifecycle.using(client.consumer(QueueName("blah"), handler, actionOnFailure = DeadLetter)) { _ =>
      channel.consumers should have size 1
      val msg = Payload.from("Hello World!")

      handler.nextResponse = Future.failed(new RuntimeException("Blah"))
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      channel.transmittedCommands.last shouldBe a[Basic.Nack]
      channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
    }
  }

  private def createClient(channel: StubChannel): RawAmqpClient[Lifecycle] =
    new LifecycleRawAmqpClient(NoOpLifecycle(channel))

}
