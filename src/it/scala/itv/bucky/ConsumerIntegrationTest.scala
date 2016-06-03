package itv.bucky

import itv.akka.rmq.{AmqpMessageProperties, AmqpMessage}
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.Blob
import org.scalatest.{Inside, FunSuite}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures
import Inside._

import scala.collection.JavaConverters._

class ConsumerIntegrationTest extends FunSuite with ScalaFutures {

  lazy val (Seq(consumerQueue, rawConsumerQueue, headersQueue), amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(
    List("bucky-consumer-test" , "bucky-consumer-raw-test", "bucky-consumer-headers-test").map(QueueName) : _*)


  case class Message(value: String)
  implicit val messageDeserializer = new BlobDeserializer[Message] {
    override def apply(blob: Blob): DeserializerResult[Message] = DeserializerResult.Success(Message(blob.to[String]))
  }
  
  test("Can consume messages from a (pre-existing) queue") {
    consumerQueue.purge()

    val handler = new StubConsumeHandler[Message]()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(QueueName(consumerQueue.name), AmqpClient.handlerOf(handler))
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

  test("Can consume messages from a (pre-existing) queue with the raw consumer") {
    rawConsumerQueue.purge()

    val handler = new StubConsumeHandler[Delivery]()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(QueueName(rawConsumerQueue.name), handler)
    } {
      handler.receivedMessages shouldBe 'empty

      val msg = Blob from "Hello World!"
      rawConsumerQueue.publish(msg)

      eventually {
        handler.receivedMessages should have size 1
        inside(handler.receivedMessages.head) {
          case Delivery(body, _, _, _) => body shouldBe msg
        }
      }
    }
  }

  test("Message headers are exposed to (raw) consumers") {
    headersQueue.purge()

    val handler = new StubConsumeHandler[Delivery]()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(QueueName(headersQueue.name), handler)
    } {
      handler.receivedMessages shouldBe 'empty

      val msg = Blob from "Hello World!"
      headersQueue.publishRaw(AmqpMessage(msg, AmqpMessageProperties(Some(Map("hello" -> "world")))))

      eventually {
        handler.receivedMessages should have size 1
        inside(handler.receivedMessages.head) {
          case Delivery(body, _, _, properties) => properties.getHeaders.asScala.get("hello").map(_.toString) shouldBe Some("world")
        }
      }
    }
  }

}
