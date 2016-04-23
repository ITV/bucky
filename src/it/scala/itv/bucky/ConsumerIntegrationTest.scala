package itv.bucky

import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.Blob
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures

class ConsumerIntegrationTest extends FunSuite with ScalaFutures {

  lazy val (Seq(consumerQueue, rawConsumerQueue), amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp("bucky-consumer-test", "bucky-consumer-raw-test")


  case class Message(value: String)
  implicit val messageDeserializer = new BlobDeserializer[Message] {
    override def apply(blob: Blob): DeserializerResult[Message] = DeserializerResult.Success(Message(blob.to[String]))
  }
  ignore("Can consume messages from a (pre-existing) queue") {
    consumerQueue.purge()

    val handler = new StubHandler[Message]()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(consumerQueue.name, AmqpClient.handlerOf(handler))
    } {
      handler.receivedMessages shouldBe 'empty

      val msg = Blob from "Hello World!"
      consumerQueue.publish(msg)

      eventually {
        handler.receivedMessages should have size 1

        handler.receivedMessages.head shouldBe Message("Hello World!")
      }
    }
  }

  ignore("Can consume messages from a (pre-existing) queue with the raw consumer") {
    rawConsumerQueue.purge()

    val handler = new StubHandler[Delivery]()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(rawConsumerQueue.name, handler)
    } {
      handler.receivedMessages shouldBe 'empty

      val msg = Blob from "Hello World!"
      rawConsumerQueue.publish(msg)

      eventually {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head shouldBe msg
      }
    }
  }
}
