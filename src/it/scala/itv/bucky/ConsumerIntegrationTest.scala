package itv.bucky

import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.Blob
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures

class ConsumerIntegrationTest extends FunSuite with ScalaFutures {

  val testQueueName = "bucky-consumer-test"
  lazy val (testQueue, amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(testQueueName)

  test("Can consume messages from a (pre-existing) queue") {
    testQueue.purge()

    val handler = new StubHandler()
    for {
      amqpClient <- amqpClientConfig
      consumer <- amqpClient.consumer(testQueueName, handler)
    } {
      handler.receivedMessages shouldBe 'empty

      val msg = Blob from "Hello World!"
      testQueue.publish(msg)

      eventually {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head shouldBe msg
      }
    }

  }

}
