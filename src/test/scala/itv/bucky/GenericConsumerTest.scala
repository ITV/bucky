package itv.bucky

import com.rabbitmq.client.impl.AMQImpl.Basic
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}
import itv.utils.Blob
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

class GenericConsumerTest extends FunSuite with ScalaFutures {

  import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

  test("Runs callback with delivered messages") {
    val channel = new StubChannel()
    val client = createClient(channel)

    import DeserializerResult._
    implicit val deserializer : BlobDeserializer[Blob] = new BlobDeserializer[Blob] {
      override def apply(message: Blob): DeserializerResult[Blob] = message.success
    }

    val handler = new StubConsumeHandler[Blob]()

    Lifecycle.using(client.consumer(QueueName("blah"), AmqpClient.handlerOf[Blob](handler))) { _ =>
      channel.consumers should have size 1
      val msg = Blob.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      handler.receivedMessages should have size 2
    }
  }

  test("should fail when there is a deserialization problem") {
    val channel = new StubChannel()
    val client = createClient(channel)

    import DeserializerResult._
    implicit val deserializer : BlobDeserializer[Blob] = new BlobDeserializer[Blob] {
      override def apply(message: Blob): DeserializerResult[Blob] = "There is a problem".failure
    }

    val handler = new StubConsumeHandler[Blob]()

    Lifecycle.using(client.consumer(QueueName("blah"), AmqpClient.handlerOf[Blob](handler))) { _ =>
      channel.consumers should have size 1
      val msg = Blob.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      channel.transmittedCommands.last shouldBe a[Basic.Nack]

      handler.receivedMessages should have size 0
    }
  }

  test("should perform deserialization action when there is a an exception during deserialization") {
    val channel = new StubChannel()
    val client = createClient(channel)

    import DeserializerResult._
    implicit val deserializer : BlobDeserializer[Blob] = new BlobDeserializer[Blob] {
      override def apply(message: Blob): DeserializerResult[Blob] = "There is a problem".failure
    }

    val handler = new StubConsumeHandler[Blob]()

    Lifecycle.using(client.consumer(QueueName("blah"), AmqpClient.handlerOf[Blob](handler, Ack), DeadLetter)) { _ =>
      channel.consumers should have size 1
      val msg = Blob.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      channel.transmittedCommands.last shouldBe a[Basic.Ack]

      handler.receivedMessages should have size 0
    }
  }

  test("should nack by default when there is a an exception during deserialization") {
    val channel = new StubChannel()
    val client = createClient(channel)

    implicit val deserializer : BlobDeserializer[Blob] = new BlobDeserializer[Blob] {
      override def apply(message: Blob): DeserializerResult[Blob] = throw new RuntimeException("Oh no")
    }

    val handler = new StubConsumeHandler[Blob]()

    Lifecycle.using(client.consumer(QueueName("blah"), AmqpClient.handlerOf[Blob](handler))) { _ =>
      channel.consumers should have size 1
      val msg = Blob.from("Hello World!")

      channel.deliver(new Basic.Deliver(channel.consumers.head.getConsumerTag, 1L, false, "exchange", "routingKey"), msg)

      channel.transmittedCommands.last shouldBe a[Basic.Nack]

      handler.receivedMessages should have size 0
    }
  }

  private def createClient(channel: StubChannel): RawAmqpClient = {
    new RawAmqpClient(NoOpLifecycle(channel), ConsumerTag("foo"))
  }


}
