package com.itv.bucky.task

import java.io.IOException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import com.itv.bucky.AtomicRef.Ref
import com.itv.bucky.task.TaskExt._
import com.itv.bucky._
import com.rabbitmq.client.AMQP.Basic.Publish
import com.rabbitmq.client.AMQP.Confirm.Select
import com.rabbitmq.client._
import com.rabbitmq.client.impl.AMQImpl
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scalaz.\/
import scalaz.concurrent.Task


class PublisherTest extends FunSuite with ScalaFutures {

  import BuckyUtils._
  import Eventually._

  test("Publishing only returns success once publication is acknowledged with Id") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._

      channel.transmittedCommands should have size 1
      channel.transmittedCommands.last shouldBe an[AMQP.Confirm.Select]

      val task = publish(anyPublishCommand())

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

      val task = publish(anyPublishCommand())

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

      val task = publish(anyPublishCommand())

      task shouldBe 'running

      channel.replyWith(new AMQImpl.Basic.Nack(1L, false, false))

      task.failure.getMessage should include("Nack")
    }
  }


  test("Only futures corresponding to acknowledged publications are completed") {
    withPublisher(timeout = 100.hours) { publisher =>
      import publisher._

      val task1 = publish(anyPublishCommand())
      val task2 = publish(anyPublishCommand())
      val task3 = publish(anyPublishCommand())

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

        val task1 = publish(anyPublishCommand())
        val task2 = publish(anyPublishCommand())
        val task3 = publish(anyPublishCommand())

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

        val task = publish(anyPublishCommand())

        eventually {
          task.failure.getMessage should ===(expectedMsg)
        }
      }
    }

    test("Publisher times out if configured to do so") {

      withPublisher(timeout = 10.millis) { publisher =>
        import publisher._

        val result = publish(anyPublishCommand())

        eventually {
          result.failure shouldBe a[TimeoutException]
        }
      }
    }

  case class TestPublisher(channel: StubChannel, publisher: Publisher[Task, PublishCommand]) {


    def publish(command: PublishCommand): TaskStatus = {
      val status = TaskStatus(new AtomicReference[Option[TaskResult]](None))
      publisher(anyPublishCommand()).unsafePerformAsync { result =>
        status.complete(result)
      }
      status
    }

  }


  case class TaskStatus(status: Ref[Option[TaskResult]]) {
    def complete(result: \/[Throwable, Unit]) = status.set(Some(result))

    def isRunning = status.get().isEmpty
    def isCompleted = status.get().isDefined

    def isSuccess: Boolean = status.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Boolean](
        (e: Throwable) =>
          fail(s"It should not fail")
        ,
        _ => true
      )
    }


    def failure: Throwable = status.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Throwable](
        identity
        ,
        _ => fail("It should not be completed successfully")
      )
    }

  }

  private def withPublisher(timeout: FiniteDuration = 1.second, channel: StubChannel = new StubChannel)(f: TestPublisher => Unit): Unit = {

    val client = TaskAmqpClient(channel)

    val publish = client.publisher(timeout)
    f(TestPublisher(channel, publish))
  }

}
