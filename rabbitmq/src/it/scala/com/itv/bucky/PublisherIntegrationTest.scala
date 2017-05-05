package com.itv.bucky

import com.itv.bucky.IntegrationUtils.defaultDeclaration
import com.itv.bucky.SameThreadExecutionContext.implicitly
import com.itv.bucky.decl.DeclarationExecutor
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._

class PublisherIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {
  implicit val consumerPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)

  import TestUtils._

  import TestLifecycle._

  test("Can publish messages to a (pre-existing) queue") {
    val testQueueName = "bucky-publisher-test"
    val routingKey = RoutingKey(testQueueName)
    val exchange = ExchangeName("")
    val handler = new QueueWatcher[Delivery]
    Lifecycle.using(rawConsumer(QueueName(testQueueName), handler)) { publisher =>
      val body = Payload.from("Hello World!")
      publisher(PublishCommand(ExchangeName(""), routingKey, MessageProperties.textPlain, body)).asTry.futureValue shouldBe 'success

      handler.nextMessage().futureValue.body.value shouldBe body.value
    }
  }

  test("Can publish messages to a (pre-existing) queue with Id") {
    val testQueueName = "bucky-publisher-test-2"
    val routingKey = RoutingKey(testQueueName)
    val exchange = ExchangeName("")

    val handler = new QueueWatcher[Delivery]

    val amqpClient = IdAmqpClient(defaultConfig)

    DeclarationExecutor(defaultDeclaration(QueueName(testQueueName)), amqpClient, 5.seconds)
    amqpClient.consumer(QueueName(testQueueName), handler)

    val publisher = amqpClient.publisher()
    val body = Payload.from("Hello World!")
    publisher(PublishCommand(ExchangeName(""), routingKey, MessageProperties.textPlain, body)).asTry.futureValue shouldBe 'success

    handler.nextMessage().futureValue.body.value shouldBe body.value

    IdAmqpClient.closeAll(amqpClient)
  }


}
