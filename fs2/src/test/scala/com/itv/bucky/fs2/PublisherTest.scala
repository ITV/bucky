package com.itv.bucky.fs2

import java.io.IOException
import java.util.concurrent.TimeoutException

import cats.effect.IO
import com.itv.bucky._
import com.rabbitmq.client.AMQP.Basic.Publish
import com.rabbitmq.client.AMQP.Confirm.Select
import com.rabbitmq.client._
import com.rabbitmq.client.impl.AMQImpl
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually

import scala.concurrent.duration._

class PublisherTest extends FunSuite {
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 100.millis)

  import TestIOExt._

  test("Publishing only returns success once publication is acknowledged with Id") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._
      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val task = publish(Any.publishCommand()).status

      eventually {
        channel.transmittedCommands should have size 2
        channel.transmittedCommands.last shouldBe an[AMQP.Basic.Publish]
      }

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

      eventually {
        task shouldBe 'success
      }
    }
  }

  test("Publishing only returns success once publication is acknowledged") {
    withPublisher() { publisher =>
      import publisher._
      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[Select]

      val task = publish(Any.publishCommand()).status

      eventually {
        channel.transmittedCommands should have size 2
        channel.transmittedCommands.last shouldBe an[Publish]
      }

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

      eventually {
        task shouldBe 'success
      }
    }
  }

  test("Negative acknowledgements result in failed future") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._
      val task = publish(Any.publishCommand()).status

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Nack(1L, false, false))

      eventually {
        task.failure.getMessage should include("Nack")
      }
    }
  }

  ignore("Only futures corresponding to acknowledged publications are completed") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._
      val task1 = publish(Any.publishCommand()).status
      val task2 = publish(Any.publishCommand()).status
      val task3 = publish(Any.publishCommand()).status

      task1 shouldBe 'running
      task2 shouldBe 'running
      task3 shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(2L, true))

      eventually {
        task1 shouldBe 'completed
      }
      eventually {
        task2 shouldBe 'completed
      }
      task3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

      eventually {
        task1 shouldBe 'completed
        task2 shouldBe 'completed
        task3 shouldBe 'completed
      }
    }
  }

  ignore("Only futures corresponding to acknowledged publications are completed: negative acknowledgment (nack)") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._
      val task1 = publish(Any.publishCommand()).status
      val task2 = publish(Any.publishCommand()).status
      val task3 = publish(Any.publishCommand()).status

      task1 should not be 'completed
      task2 should not be 'completed
      task3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Nack(2L, true, false))

      eventually {
        task1 shouldBe 'completed
      }
      eventually {
        task2 shouldBe 'completed
      }

      task3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

      task1 shouldBe 'completed
      task2 shouldBe 'completed
      eventually {
        task3 shouldBe 'completed
      }
    }
  }

  test("Cannot publish when there is a IOException") {
    val expectedMsg = "There is a network problem"
    val mockCannel = new StubChannel {
      override def basicPublish(exchange: String,
                                routingKey: String,
                                mandatory: Boolean,
                                immediate: Boolean,
                                props: AMQP.BasicProperties,
                                body: Array[Byte]): Unit =
        throw new IOException(expectedMsg)

    }
    withPublisher(timeout = 100.hours, channel = mockCannel) { publisher =>
      import publisher._
      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val status = publish(Any.publishCommand()).status

      eventually {
        status shouldBe 'failure
      }
      status.failure.getMessage should ===(expectedMsg)
    }
  }

  test("Publisher times out if configured to do so") {

    withPublisher(timeout = 10.millis) { publisher =>
      import publisher._
      val result = publish(Any.publishCommand()).status

      eventually {
        result.failure shouldBe a[TimeoutException]
      }
    }
  }

  case class TestPublisher(channel: StubChannel, publish: Publisher[IO, PublishCommand])

  def withPublisher(timeout: FiniteDuration = 1.second, channel: StubChannel = new StubChannel)(
      f: TestPublisher => Unit): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val client  = IOAmqpClient(channel)
    val publish = client.publisher(timeout)
    f(TestPublisher(channel, publish))
  }

}
