package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.contentdelivery.testutilities.rmq.{MessageQueue, BrokerConfig}
import itv.httpyroraptor.{UriBuilder, PUT, SyncHttpClient}
import itv.utils.Blob
import org.scalatest.Matchers._

object IntegrationUtils {

  def setUp(testQueueNames: String*) = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    val amqpClientConfig = AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
    val adminConfig = config.getConfig("rmq.admin-api")
    val rmqAdminConfig = BrokerConfig(adminConfig.getString("username"), adminConfig.getString("password"), host, adminConfig.getInt("port"))

    val rmqAdminHhttp = SyncHttpClient.forHost(rmqAdminConfig.hostname, rmqAdminConfig.port).withAuthentication(rmqAdminConfig.username, rmqAdminConfig.password)

    val testQueues = testQueueNames.map { testQueueName =>
      rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / testQueueName).body("application/json", Blob.from(
        """{"auto_delete": "true", "durable": "true", "arguments": {"x-expires": 60000}}"""))) shouldBe 'successful
      MessageQueue(testQueueName, rmqAdminConfig)
    }
    (testQueues, amqpClientConfig, rmqAdminHhttp)
  }
  
}
