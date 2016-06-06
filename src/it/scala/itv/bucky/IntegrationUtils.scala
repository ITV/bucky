package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.bucky.decl.{Declaration, Binding, Exchange, Queue}
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.rmq.{BrokerConfig, MessageQueue}
import itv.httpyroraptor._
import itv.utils.Blob
import org.scalatest.Matchers._
import scala.concurrent.{Await, Future}
import scalaz.Id
import scalaz.Id.Id
import scala.concurrent.duration._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

object IntegrationUtils {

  def setUp(testQueueNames: QueueName*): (Seq[MessageQueue], AmqpClientConfig, HttpClient[Id]) = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHhttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    val testQueues = testQueueNames.map { testQueueName =>
      rmqAdminHhttp.handle(PUT(UriBuilder / "api" / "queues" / "/" / testQueueName.value).body("application/json", Blob.from(
        """{"auto_delete": "true", "durable": "true", "arguments": {"x-expires": 120000}}"""))) shouldBe 'successful
      MessageQueue(testQueueName.value, rmqAdminConfig)
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

  def declareQueue(name: String): Lifecycle[(MessageQueue, MessageQueue, MessageQueue)] = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    val mainQueue = QueueName(name)
    val deadletterQueue = QueueName(s"$name.dlq")
    val requeueQueue = QueueName(s"$name.requeue")

    val dlx = ExchangeName(s"$name.dlx")
    val requeueExchange = ExchangeName(s"$name.requeue")
    val redeliverExchange = ExchangeName(s"$name.redeliver")

    val queues = List(Queue(mainQueue).autoDelete.deadLetterExchange(dlx).expires(1.minute),
      Queue(deadletterQueue).autoDelete.expires(1.minute),
      Queue(requeueQueue).autoDelete.deadLetterExchange(redeliverExchange).expires(1.minute).messageTTL(1.second))

    val routingKey = RoutingKey(name)

    val exchanges = List(Exchange(dlx).expires(1.minute).binding(routingKey -> deadletterQueue),
      Exchange(requeueExchange).expires(1.minute).binding(routingKey -> requeueQueue),
      Exchange(redeliverExchange).expires(1.minute).binding(routingKey -> mainQueue))

    for {
      client <- amqpClientConfig
      result = Declaration.applyAll(queues ++ exchanges, client)
      _ = Await.result(result, 5.seconds)
    }
      yield (MessageQueue(mainQueue.value, rmqAdminConfig),
        MessageQueue(requeueQueue.value, rmqAdminConfig),
        MessageQueue(deadletterQueue.value, rmqAdminConfig))
  }


}
