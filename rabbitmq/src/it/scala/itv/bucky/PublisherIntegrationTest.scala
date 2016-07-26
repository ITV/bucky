package itv.bucky

import com.typesafe.scalalogging.StrictLogging
import itv.bucky.SameThreadExecutionContext.implicitly
import itv.bucky.TestUtils._
import itv.contentdelivery.lifecycle.Lifecycle
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

class PublisherIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {

  val testQueueName = "bucky-publisher-test"
  val routingKey = RoutingKey(testQueueName)
  val exchange = ExchangeName("")

  import TestLifecycle._

  test("Can publish messages to a (pre-existing) queue") {
    val handler = new QueueWatcher[Delivery]
    Lifecycle.using(rawConsumer(QueueName(testQueueName), handler)) { publisher =>
      val body = Payload.from("Hello World!")
      publisher(PublishCommand(ExchangeName(""), routingKey, MessageProperties.textPlain, body)).asTry.futureValue shouldBe 'success

      handler.nextMessage().futureValue.body.value shouldBe body.value
    }
  }

}
