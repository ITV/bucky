package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.contentdelivery.testutilities.rmq.{BrokerConfig, MessageQueue}
import itv.httpyroraptor._
import itv.utils.Blob
import org.scalatest.Matchers._
import scalaz.Id
import scalaz.Id.Id

object IntegrationUtils {

  def setUp(testQueueNames: String*): (Seq[MessageQueue], AmqpClientConfig, HttpClient[Id]) = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHhttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    val testQueues = testQueueNames.map { testQueueName =>
      rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / testQueueName).body("application/json", Blob.from(
        """{"auto_delete": "true", "durable": "true", "arguments": {"x-expires": 120000}}"""))) shouldBe 'successful
      MessageQueue(testQueueName, rmqAdminConfig)
    }
    (testQueues, amqpClientConfig, rmqAdminHhttp)
  }

  def configAndHttp: (AmqpClientConfig, BrokerConfig, AuthenticatedHttpClient[Id.Id]) = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    val amqpClientConfig = AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
    val adminConfig = config.getConfig("rmq.admin-api")
    val rmqAdminConfig = BrokerConfig(adminConfig.getString("username"), adminConfig.getString("password"), host, adminConfig.getInt("port"))

    val rmqAdminHhttp = SyncHttpClient.forHost(rmqAdminConfig.hostname, rmqAdminConfig.port).withAuthentication(rmqAdminConfig.username, rmqAdminConfig.password)
    (amqpClientConfig, rmqAdminConfig, rmqAdminHhttp)
  }

  def defineDeadlettering(queueName: String) = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")
    val adminConfig = config.getConfig("rmq.admin-api")
    val requeueQueue = queueName + ".requeue"
    val rmqAdminConfig = BrokerConfig(adminConfig.getString("username"), adminConfig.getString("password"), host, adminConfig.getInt("port"))
    val rmqAdminHhttp = SyncHttpClient.forHost(rmqAdminConfig.hostname, rmqAdminConfig.port).withAuthentication(rmqAdminConfig.username, rmqAdminConfig.password)

    rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / requeueQueue).body("application/json", Blob.from(
      """{"auto_delete": "true", "durable": "true", "arguments": {"x-expires": 120000}}"""))) shouldBe 'successful

    rmqAdminHhttp.handle(POST(UriBuilder / "api" / "bindings" / "/" / "e" / s"$queueName.redeliver" / "q" / queueName).body("application/json", Blob.from(
      s"""{"routing_key":"$queueName","arguments":{}}"""))) shouldBe 'successful

    val response = rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "policies" / "/" / "bucky-requeue").body("application/json", Blob.from(
      s"""{"pattern":"$queueName.requeue$$", "definition": {"dead-letter-exchange":	"$queueName.redeliver", "ha-mode":	"all", "ha-sync-mode": "automatic"}, "priority":100, "apply-to": "queues"}"""))) shouldBe 'successful
  }

  def declareQueue(name: String): (MessageQueue, MessageQueue, MessageQueue) = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / name).body("application/json", Blob.from(
      s"""{"auto_delete": "true", "durable": "true", "arguments": {"x-dead-letter-exchange": "$name.dlx", "x-expires": 60000}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / s"$name.requeue").body("application/json", Blob.from(
      s"""{"auto_delete": "true", "durable": "true", "arguments": {"x-dead-letter-exchange": "$name.redeliver", "x-expires": 60000, "x-message-ttl": 1000}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / s"$name.dlq").body("application/json", Blob.from(
      """{"auto_delete": "true", "durable": "true", "arguments": {"x-expires": 60000}}"""))) shouldBe 'successful

    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "exchanges" / "/" / s"$name.dlx").body("application/json", Blob.from(
      """{"type":"direct","auto_delete":true,"durable":true,"internal":false,"arguments":{"x-expires": 60000}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "exchanges" / "/" / s"$name.requeue").body("application/json", Blob.from(
      """{"type":"direct","auto_delete":true,"durable":true,"internal":false,"arguments":{"x-expires": 60000}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(PUT(UriBuilder / "api" / "exchanges" / "/" / s"$name.redeliver").body("application/json", Blob.from(
      """{"type":"direct","auto_delete":true,"durable":true,"internal":false,"arguments":{"x-expires": 60000}}"""))) shouldBe 'successful

    rmqAdminHttp.handle(POST(UriBuilder / "api" / "bindings" / "/" / "e" / s"$name.redeliver" / "q" / name).body("application/json", Blob.from(
      s"""{"routing_key":"$name","arguments":{}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(POST(UriBuilder / "api" / "bindings" / "/" / "e" / s"$name.requeue" / "q" / s"$name.requeue").body("application/json", Blob.from(
      s"""{"routing_key":"$name","arguments":{}}"""))) shouldBe 'successful
    rmqAdminHttp.handle(POST(UriBuilder / "api" / "bindings" / "/" / "e" / s"$name.dlx" / "q" / s"$name.dlq").body("application/json", Blob.from(
      s"""{"routing_key":"$name","arguments":{}}"""))) shouldBe 'successful


    (MessageQueue(name, rmqAdminConfig), MessageQueue(name + ".requeue", rmqAdminConfig), MessageQueue(name + ".dlq", rmqAdminConfig))
  }


}
