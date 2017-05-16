package com.itv.bucky.taskz

import java.io.IOException
import java.util.concurrent.TimeoutException

import com.itv.bucky.taskz.TaskExt._
import com.itv.bucky._
import com.rabbitmq.client.AMQP.Basic.Publish
import com.rabbitmq.client.AMQP.Confirm.Select
import com.rabbitmq.client._
import com.rabbitmq.client.impl.AMQImpl
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._


class PublisherTest extends FunSuite with ScalaFutures {

  import Eventually._

  test("Publishing only returns success once publication is acknowledged with Id") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._

      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val task = resultFrom(publish(Any.publishCommand()))

      channel.transmittedCommands should have size 2
      channel.transmittedCommands.last shouldBe an[AMQP.Basic.Publish]

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

      task shouldBe 'success
    }
  }

  test("Publishing only returns success once publication is acknowledged") {
    withPublisher() { publisher =>
      import publisher._

      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[Select]

      val task = resultFrom(publish(Any.publishCommand()))

      channel.transmittedCommands should have size 2
      channel.transmittedCommands.last shouldBe an[Publish]

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(1L, false))

      task shouldBe 'success
    }
  }


  test("Negative acknowledgements result in failed future") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._

      val task = resultFrom(publish(Any.publishCommand()))

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Nack(1L, false, false))

      task.failure.getMessage should include("Nack")
    }
  }


  test("Only futures corresponding to acknowledged publications are completed") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._

      val task1 = resultFrom(publish(Any.publishCommand()))
      val task2 = resultFrom(publish(Any.publishCommand()))
      val task3 = resultFrom(publish(Any.publishCommand()))

      task1 shouldBe 'running
      task2 shouldBe 'running
      task3 shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Ack(2L, true))

      task1 shouldBe 'completed
      task2 shouldBe 'completed
      task3 should not be 'completed

      channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

      task1 shouldBe 'completed
      task2 shouldBe 'completed
      task3 shouldBe 'completed
    }
  }


    test("Only futures corresponding to acknowledged publications are completed: negative acknowledgment (nack)") {
      withPublisher(timeout = 100.hours) { publisher =>
        import publisher._

        val task1 = resultFrom(publish(Any.publishCommand()))
        val task2 = resultFrom(publish(Any.publishCommand()))
        val task3 = resultFrom(publish(Any.publishCommand()))

        task1 should not be 'completed
        task2 should not be 'completed
        task3 should not be 'completed

        channel.replyWith(new AMQImpl.Basic.Nack(2L, true, false))

        task1 shouldBe 'completed
        task2 shouldBe 'completed
        task3 should not be 'completed

        channel.replyWith(new AMQImpl.Basic.Ack(3L, false))

        task1 shouldBe 'completed
        task2 shouldBe 'completed
        task3 shouldBe 'completed
      }
    }

    test("Cannot publish when there is a IOException") {
      val expectedMsg = "There is a network problem"
      val mockCannel = new StubChannel {
        override def basicPublish(exchange: String, routingKey: String, mandatory: Boolean, immediate: Boolean, props: AMQP.BasicProperties, body: Array[Byte]): Unit =
          throw new IOException(expectedMsg)

      }
      withPublisher(timeout = 100.hours, channel = mockCannel) { publisher =>
        import publisher._

        channel.transmittedCommands should have size 1
        channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

        val task = resultFrom(publish(Any.publishCommand()))

        eventually {
          task.failure.getMessage should ===(expectedMsg)
        }
      }
    }

    test("Publisher times out if configured to do so") {

      withPublisher(timeout = 10.millis) { publisher =>
        import publisher._

        val result = resultFrom(publish(Any.publishCommand()))

        eventually {
          result.failure shouldBe a[TimeoutException]
        }
      }
    }

}
