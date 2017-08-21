package com.itv.bucky.future

import java.io.IOException
import java.util.concurrent.TimeoutException

import com.itv.bucky.lifecycle._
import com.itv.bucky.{Any, StubChannel}
import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}
import com.rabbitmq.client._
import com.rabbitmq.client.impl.AMQImpl
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class PublisherTest extends FunSuite with ScalaFutures {

  import FutureExt._

  test("Publishing only returns success once publication is acknowledged with Id") {
    val channel = new StubChannel()
    val client  = new FutureIdAmqpClient(channel)

    val publish = client.publisher(timeout = Duration.Inf)

    channel.transmittedCommands should have size 1
    channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

    val result = publish(Any.publishCommand())

    channel.transmittedCommands should have size 2
    channel.transmittedCommands.last shouldBe an[AMQP.Basic.Publish]

    result should not be 'completed

    channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

    result shouldBe 'completed
    result.futureValue.shouldBe(())
  }

  test("Publishing only returns success once publication is acknowledged") {
    val channel = new StubChannel()
    val client  = createClient(channel)

    Lifecycle.using(client.publisher(timeout = Duration.Inf)) { publish =>
      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val result = publish(Any.publishCommand())

      channel.transmittedCommands should have size 2
      channel.transmittedCommands.last shouldBe an[AMQP.Basic.Publish]

      result should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

      result shouldBe 'completed
      result.futureValue.shouldBe(())
    }
  }

  test("Negative acknowledgements result in failed future") {
    val channel = new StubChannel()
    val client  = createClient(channel)

    Lifecycle.using(client.publisher(timeout = Duration.Inf)) { publish =>
      val result = publish(Any.publishCommand())

      result should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Nack(1L, false, false))

      result shouldBe 'completed
      result.failed.futureValue.getMessage should include("Nack")
    }
  }

  test("Only futures corresponding to acknowledged publications are completed") {
    val channel = new StubChannel()
    val client  = createClient(channel)

    Lifecycle.using(client.publisher(timeout = Duration.Inf)) { publish =>
      val pub1 = publish(Any.publishCommand())
      val pub2 = publish(Any.publishCommand())
      val pub3 = publish(Any.publishCommand())

      pub1 should not be 'completed
      pub2 should not be 'completed
      pub3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(2L, true))

      pub1 shouldBe 'completed
      pub2 shouldBe 'completed
      pub3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

      pub1 shouldBe 'completed
      pub2 shouldBe 'completed
      pub3 shouldBe 'completed
    }
  }

  test("Only futures corresponding to acknowledged publications are completed: negative acknowledgment (nack)") {
    val channel = new StubChannel()
    val client  = createClient(channel)

    Lifecycle.using(client.publisher(timeout = Duration.Inf)) { publish =>
      val pub1 = publish(Any.publishCommand())
      val pub2 = publish(Any.publishCommand())
      val pub3 = publish(Any.publishCommand())

      pub1 should not be 'completed
      pub2 should not be 'completed
      pub3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Nack(2L, true, false))

      pub1 shouldBe 'completed
      pub2 shouldBe 'completed
      pub3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

      pub1 shouldBe 'completed
      pub2 shouldBe 'completed
      pub3 shouldBe 'completed
    }
  }

  test("Cannot publish when there is a IOException") {
    val expectedMsg = "There is a network problem"
    val channel = new StubChannel {
      override def basicPublish(exchange: String,
                                routingKey: String,
                                mandatory: Boolean,
                                immediate: Boolean,
                                props: AMQP.BasicProperties,
                                body: Array[Byte]): Unit =
        throw new IOException(expectedMsg)

    }
    val client = createClient(channel)

    Lifecycle.using(client.publisher(Duration.Inf)) { publish =>
      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val result = publish(Any.publishCommand()).failed

      whenReady(result) { exception =>
        exception.getMessage shouldBe expectedMsg
      }
    }
  }

  test("Publisher times out if configured to do so") {
    val channel = new StubChannel()
    val client  = createClient(channel)

    Lifecycle.using(client.publisher(timeout = 10.millis)) { publish =>
      val result = publish(Any.publishCommand())

      result.asTry.futureValue.failed.get shouldBe a[TimeoutException]
    }
  }

  private def createClient(channel: StubChannel): FutureAmqpClient[Lifecycle] = {
    new LifecycleRawAmqpClient(NoOpLifecycle(channel))
  }
}
