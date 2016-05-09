package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.contentdelivery.testutilities.rmq.{BrokerConfig, MessageQueue}
import itv.httpyroraptor._
import itv.utils.Blob
import org.scalatest.Matchers._
import scalaz.Id.Id

object IntegrationUtils {

  def setUp(testQueueNames: String*): (Seq[MessageQueue], AmqpClientConfig, HttpClient[Id]) = {
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

  def defineDeadlettering(queueName: String) = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")
    val adminConfig = config.getConfig("rmq.admin-api")
    val deadLetterQueueName = queueName + ".dlq"
    val rmqAdminConfig = BrokerConfig(adminConfig.getString("username"), adminConfig.getString("password"), host, adminConfig.getInt("port"))
    val rmqAdminHhttp = SyncHttpClient.forHost(rmqAdminConfig.hostname, rmqAdminConfig.port).withAuthentication(rmqAdminConfig.username, rmqAdminConfig.password)

    rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "exchanges" / "/" / "bucky.dlx").body("application/json", Blob.from(
      """{"type":"direct","auto_delete":true,"durable":true,"internal":false,"arguments":{"x-expires": 60000}}""")))

    rmqAdminHhttp.handle(POST(UriBuilder / "api" / "bindings" / "/" / "e" / "bucky.dlx" / "q" / deadLetterQueueName).body("application/json", Blob.from(
      s"""{"routing_key":"$queueName","arguments":{}}""".stripMargin)))



    val response = rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "policies" / "/" / "bucky-dead-letter").body("application/json", Blob.from(
      s"""{"pattern":"$queueName$$", "definition": {"dead-letter-exchange":	"bucky.dlx", "ha-mode":	"all", "ha-sync-mode": "automatic"}, "priority":100, "apply-to": "queues"}""")))
    response.statusCode shouldBe 204
  }


}
