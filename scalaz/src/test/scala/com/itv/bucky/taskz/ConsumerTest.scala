package com.itv.bucky.taskz

import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import com.itv.bucky._
import com.itv.bucky.future.SameThreadExecutionContext
import com.rabbitmq.client.impl.AMQImpl.Basic
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scalaz.concurrent.Task


class ConsumerTest extends FunSuite with StrictLogging {

  test(s"Runs callback with delivered messages with Id") {
    withConsumer { consumer =>
      import consumer._
      eventually {
        logger.info("Waiting for the consumer to be ready")
        channel.consumers should have size 1
        channel.setPrefetchCount shouldBe 12
      }

      val msg = Payload.from("Hello World!")
      handler.nextResponse = Task.now(Ack)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      eventually {
        logger.info("Waiting for ack")
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
      }
      handler.nextResponse = Task.now(DeadLetter)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      eventually {
        logger.info("Waiting for nack")
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }
      handler.nextResponse = Task.now(RequeueImmediately)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
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

      handler.nextResponse = Task.now(Ack)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
      }

      handler.nextResponse = Task.now(DeadLetter)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }

      handler.nextResponse = Task.now(RequeueImmediately)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
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

      handler.nextResponse = Task.fail(new RuntimeException("Blah"))
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        channel.transmittedCommands.last.asInstanceOf[Basic.Nack].getRequeue shouldBe false
      }
    }
  }

  case class TestConsumer(channel: StubChannel, handler: StubConsumeHandler[Task, Delivery])

  private def withConsumer(f: TestConsumer => Unit): Unit = {
    val channel = new StubChannel()
    implicit val pool = ExecutionContextExecutorServiceBridge(SameThreadExecutionContext)
    val client = new TaskAmqpClient(channel)(pool)

    val handler = new StubConsumeHandler[Task, Delivery]()

    val queueName = QueueName("blah")
    client.consumer(queueName, handler, prefetchCount = 12).run.unsafePerformAsync { result =>
      logger.info(s"Close consumer for $queueName: $result")
    }
    f(TestConsumer(channel, handler))
  }


  object ExecutionContextExecutorServiceBridge {
    def apply(ec: ExecutionContext): ExecutionContextExecutorService = ec match {
      case null => throw null
      case eces: ExecutionContextExecutorService => eces
      case other => new AbstractExecutorService with ExecutionContextExecutorService {
        override def prepare(): ExecutionContext = other

        override def isShutdown = false

        override def isTerminated = false

        override def shutdown() = ()

        override def shutdownNow() = Collections.emptyList[Runnable]

        override def execute(runnable: Runnable): Unit = other execute runnable

        override def reportFailure(t: Throwable): Unit = other reportFailure t

        override def awaitTermination(length: Long, unit: TimeUnit): Boolean = false
      }
    }
  }

}
