package com.itv.bucky.future

import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}
import com.rabbitmq.client.impl.AMQImpl.Basic
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class GenericConsumerTest extends FunSuite with ScalaFutures {

  import FutureExt._
  import com.itv.bucky.UnmarshalResult._

  test("Runs callback with delivered messages with Id") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_.unmarshalSuccess)
    val test                                         = testId(unmarshaller)

    test.channel.consumers should have size 1
    val msg = Payload.from("Hello World!")

    test.channel
      .deliver(new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)
    test.channel
      .deliver(new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

    test.handler.receivedMessages should have size 2
  }

  test("Runs callback with delivered messages") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_.unmarshalSuccess)
    Lifecycle.using(testLifecycle(unmarshaller)) { test =>
      test.channel.consumers should have size 1
      val msg = Payload.from("Hello World!")

      test.channel.deliver(
        new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
        msg)
      test.channel.deliver(
        new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
        msg)

      test.handler.receivedMessages should have size 2
    }
  }

  test("should fail when there is a deserialization problem") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)

    Lifecycle.using(testLifecycle(unmarshaller)) { test =>
      test.channel.consumers should have size 1
      val msg = Payload.from("Hello World!")

      test.channel.deliver(
        new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
        msg)

      test.channel.transmittedCommands.last shouldBe a[Basic.Nack]
      test.handler.receivedMessages should have size 0
    }
  }

  test("should perform deserialization action when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)

    Lifecycle.using(testLifecycle(unmarshaller, Ack, DeadLetter)) { test =>
      test.channel.consumers should have size 1
      val msg = Payload.from("Hello World!")
      test.channel.deliver(
        new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
        msg)
      test.channel.transmittedCommands.last shouldBe a[Basic.Ack]
      test.handler.receivedMessages should have size 0
    }
  }

  test("should nack by default when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => throw new RuntimeException("Oh No!"))

    Lifecycle.using(testLifecycle(unmarshaller)) { test =>
      test.channel.consumers should have size 1
      val msg = Payload.from("Hello World!")

      test.channel.deliver(
        new Basic.Deliver(test.channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
        msg)
      test.channel.transmittedCommands.last shouldBe a[Basic.Nack]
      test.handler.receivedMessages should have size 0
    }
  }

  case class TestFixture(channel: StubChannel, handler: StubConsumeHandler[Future, Payload])

  def testId(unmarshaller: Unmarshaller[Payload, Payload],
             unmarshalFailureAction: ConsumeAction = DeadLetter,
             actionOnFailure: ConsumeAction = DeadLetter): Id[TestFixture] = {
    import Monad._
    val channel = new StubChannel()
    val client  = new FutureIdAmqpClient(channel)
    val handler = new StubConsumeHandler[Future, Payload]()

    val of1: Handler[Future, Delivery] = AmqpClient.handlerOf(handler, unmarshaller, unmarshalFailureAction)
    client.consumer(QueueName("blah"), of1, actionOnFailure).flatMap(_ => TestFixture(channel, handler))
  }

  def testLifecycle(unmarshaller: Unmarshaller[Payload, Payload],
                    unmarshalFailureAction: ConsumeAction = DeadLetter,
                    actionOnFailure: ConsumeAction = DeadLetter): Lifecycle[TestFixture] = {
    val channel = new StubChannel()
    val client  = createClient(channel)
    val handler = new StubConsumeHandler[Future, Payload]()

    val of1: Handler[Future, Delivery] = AmqpClient.handlerOf(handler, unmarshaller, unmarshalFailureAction)
    client.consumer(QueueName("blah"), of1, actionOnFailure).map(_ => TestFixture(channel, handler))
  }

  private def createClient(channel: StubChannel): FutureAmqpClient[Lifecycle] = {
    new LifecycleRawAmqpClient(NoOpLifecycle(channel))
  }

}
