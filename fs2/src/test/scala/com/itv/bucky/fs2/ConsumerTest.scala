package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._
import com.rabbitmq.client.impl.AMQImpl.Basic
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._

class ConsumerTest extends FunSuite with StrictLogging {
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  test(s"Runs callback with delivered messages with Id") {
    withConsumer { consumer =>
      import consumer._
      eventually {
        logger.info("Waiting for the consumer to be ready")
        channel.consumers should have size 1
        channel.setPrefetchCount shouldBe 12
      }

      val msg = Payload.from("Hello World!")
      handler.nextResponse = IO.pure(Ack)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        logger.info("Waiting for ack")
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
      }
      handler.nextResponse = IO.pure(DeadLetter)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        logger.info("Waiting for nack")
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }
      handler.nextResponse = IO.pure(RequeueImmediately)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        logger.info("Waiting for nack")
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe true
      }
      handler.receivedMessages should have size 3
    }
  }

  test("Runs callback with delivered messages") {
    withConsumer { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
        channel.setPrefetchCount shouldBe 12
      }

      val msg = Payload.from("Hello World!")

      handler.nextResponse = IO.pure(Ack)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
      }

      handler.nextResponse = IO.pure(DeadLetter)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }

      handler.nextResponse = IO.pure(RequeueImmediately)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)
      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe true
      }

      handler.receivedMessages should have size 3
    }
  }

  test("Should send exceptionalAction when an exception occurs in the handler") {
    withConsumer { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      handler.nextResponse = IO.raiseError(new RuntimeException("Blah"))
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }
    }
  }

  case class TestConsumer(channel: StubChannel, handler: StubConsumeHandler[IO, Delivery])
  private def withConsumer(f: TestConsumer => Unit): Unit = {
    val channel = new StubChannel()
    val client  = IOAmqpClient(channel)

    val handler = new StubConsumeHandler[IO, Delivery]()

    val queueName = QueueName("blah")
    client.consumer(queueName, handler, prefetchCount = 12).run.unsafeRunAsync { result =>
      logger.info(s"Close consumer for $queueName: $result")
    }
    f(TestConsumer(channel, handler))
  }

}
