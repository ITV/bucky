package com.itv.bucky.taskz

import java.util.concurrent.Executors

import com.itv.bucky._
import com.rabbitmq.client.impl.AMQImpl.Basic
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._

import scala.language.higherKinds
import scalaz.concurrent.Task

class GenericConsumerTest extends FunSuite with StrictLogging {

  import com.itv.bucky.UnmarshalResult._

  test("Runs callback with delivered messages") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_.unmarshalSuccess)

    wihConsumer(unmarshaller) { consumer =>
      import consumer._

      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        handler.receivedMessages should have size 2
      }
    }
  }


  test("should fail when there is a deserialization problem") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)

    wihConsumer(unmarshaller) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        handler.receivedMessages should have size 0
      }
    }
  }

  test("should perform deserialization action when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)
    wihConsumer(unmarshaller, Ack, DeadLetter) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
        handler.receivedMessages should have size 0
      }
    }
  }


  test("should nack by default when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_ => throw new RuntimeException("Oh No!"))

    wihConsumer(unmarshaller) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        handler.receivedMessages should have size 0
      }
    }
  }

  case class TestConsumer(channel: StubChannel, handler: StubConsumeHandler[Task, Payload])

  def wihConsumer(unmarshaller: Unmarshaller[Payload, Payload], unmarshalFailureAction: ConsumeAction = DeadLetter, actionOnFailure: ConsumeAction = DeadLetter)
                 (f: TestConsumer => Unit): Unit = {
    val channel = new StubChannel()
    implicit val pool = Executors.newSingleThreadExecutor()
    val client = new TaskAmqpClient(channel)
    val handler = new StubConsumeHandler[Task, Payload]()

    val of1: Handler[Task, Delivery] = AmqpClient.handlerOf(handler, unmarshaller, unmarshalFailureAction)
    val queueName = QueueName("blah")
    client.consumer(queueName, of1, actionOnFailure).run.unsafePerformAsync { result =>
      logger.info(s"Close consumer for $queueName: $result")
    }

    f(TestConsumer(channel, handler))
  }


}
