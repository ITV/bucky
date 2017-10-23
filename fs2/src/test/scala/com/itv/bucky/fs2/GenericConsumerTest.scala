package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._
import com.rabbitmq.client.impl.AMQImpl.Basic
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

class GenericConsumerTest extends FunSuite with StrictLogging {

  import com.itv.bucky.future.SameThreadExecutionContext.implicitly
  import com.itv.bucky.UnmarshalResult._

  implicit val patienceConfig: Eventually.PatienceConfig =
    Eventually.PatienceConfig(timeout = 5 seconds, interval = 100 millis)

  test(s"Runs callback with delivered messages") {
    val unmarshaller: Unmarshaller[Payload, Payload] = Unmarshaller.liftResult(_.unmarshalSuccess)

    wihConsumer(unmarshaller) { consumer =>
      import consumer._

      eventually {
        logger.info(s"Wait for consumer to be ready!")
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      val consumerTag = channel.consumers.headOption.fold(fail(s"No consumer present!"))(_.getConsumerTag)
      logger.info(s"Deliver the first message")
      channel.deliver(new Basic.Deliver(consumerTag, 1L, false, "exchange", "routingKey"), msg)
      logger.info(s"Deliver the second message")
      channel.deliver(new Basic.Deliver(consumerTag, 1L, false, "exchange", "routingKey"), msg)

      eventually {
        logger.info(s"Wait until messages has been recieved")
        handler.receivedMessages should have size 2
      }
    }
  }

  test("should fail when there is a deserialization problem") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)

    wihConsumer(unmarshaller) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Nack]
        handler.receivedMessages should have size 0
      }
    }
  }

  test("should perform deserialization action when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => "There is a problem".unmarshalFailure)
    wihConsumer(unmarshaller, Ack, DeadLetter) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")
      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)

      eventually {
        channel.transmittedCommands.last shouldBe a[Basic.Ack]
        handler.receivedMessages should have size 0
      }
    }
  }

  test("should nack by default when there is a an exception during deserialization") {
    val unmarshaller: Unmarshaller[Payload, Payload] =
      Unmarshaller.liftResult(_ => throw new RuntimeException("Oh No!"))

    wihConsumer(unmarshaller) { consumer =>
      import consumer._
      eventually {
        channel.consumers should have size 1
      }
      val msg = Payload.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"),
                      msg)

      eventually {
        withClue("transmittedCommands are " + channel.transmittedCommands) {
          channel.transmittedCommands.last shouldBe a[Basic.Nack]
        }
        handler.receivedMessages should have size 0
      }
    }
  }

  case class TestConsumer(channel: StubChannel, handler: StubConsumeHandler[IO, Payload])

  def wihConsumer(unmarshaller: Unmarshaller[Payload, Payload],
                  unmarshalFailureAction: ConsumeAction = DeadLetter,
                  actionOnFailure: ConsumeAction = DeadLetter)(f: TestConsumer => Unit): Unit = {
    val channel = new StubChannel()
    val client  = IOAmqpClient(channel)
    val handler = new StubConsumeHandler[IO, Payload]()

    val of1: Handler[IO, Delivery] = AmqpClient.handlerOf(handler, unmarshaller, unmarshalFailureAction)
    val queueName                    = QueueName("blah")
    client.consumer(queueName, of1, actionOnFailure).run.unsafeRunAsync { result =>
      logger.info(s"Close consumer for $queueName: $result")
    }

    f(TestConsumer(channel, handler))
  }
}
