package itv.bucky

import itv.bucky.TestUtils._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.json.JsonResult
import itv.httpyroraptor._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import itv.bucky.SameThreadExecutionContext.implicitly

class PublisherIntegrationTest extends FunSuite with ScalaFutures {

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

  test("Publisher can recover from connection failure") {
    import IntegrationUtils._
    lazy val (testQueue, amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.declareQueues(QueueName(testQueueName))
    testQueue.head.purge()
    val config = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest", networkRecoveryInterval = Some(500.millis))



    Lifecycle.using(base(defaultDeclaration(QueueName(testQueueName)), config)) { case (_, publisher) =>
      val body = Payload.from("Hello World!")
      publisher(PublishCommand(ExchangeName(""), routingKey, MessageProperties.persistentBasic, body)).asTry.futureValue shouldBe 'success

      killRabbitConnection(config)

      // Publish fails until connection is re-established
      publisher(PublishCommand(exchange, routingKey, MessageProperties.persistentBasic, Payload.from("Immediately after"))).asTry.futureValue shouldBe 'failure

      // Publish succeeds once connection is re-established
      Thread.sleep(600L)

      publisher(PublishCommand(exchange, routingKey, MessageProperties.persistentBasic, Payload.from("A while after"))).asTry.futureValue shouldBe 'success
      testQueue.head.consumeAllMessages() should have size 2

    }

  }

  private def killRabbitConnection(config: AmqpClientConfig): Unit = {
    val rmqAdminHhttp = SyncHttpClient.forHost(config.host, 15672).withAuthentication(config.username, config.password)
    val response = rmqAdminHhttp.handle(GET("/api/connections"))

    println(response)

    val jsonResult = response.body.to[JsonResult]
    for {
      connection <- jsonResult.array if connection("user").string == defaultConfig.username
    } {
      val connectionName = connection("name").string
      println(s"Killing connection $connectionName")
      rmqAdminHhttp.handle(DELETE(UriBuilder / "api" / "connections" / connectionName)) shouldBe 'successful
    }
  }


}
