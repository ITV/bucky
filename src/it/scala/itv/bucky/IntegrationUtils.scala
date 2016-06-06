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

    val oneMinute = Int.box(60000)
    val oneSecond = Int.box(1000)

    val queues = List(Queue(mainQueue,
      durable = true,
      exclusive = false,
      autoDelete = true,
      Map("x-dead-letter-exchange" -> s"$name.dlx", "x-expires" -> oneMinute)),
      Queue(deadletterQueue,
        durable = true,
        exclusive = false,
        autoDelete = true,
        Map("x-expires" -> oneMinute)),
      Queue(requeueQueue,
        durable = true,
        exclusive = false,
        autoDelete = true,
        Map("x-dead-letter-exchange" -> s"$name.redeliver", "x-expires" -> oneMinute, "x-message-ttl" -> oneSecond)))

    val deadletterExchange = ExchangeName(s"$name.dlx")
    val requeueExchange = ExchangeName(s"$name.requeue")
    val redeliverExchange = ExchangeName(s"$name.redeliver")

    val exchanges = List(Exchange(deadletterExchange,
      durable = false,
      autoDelete = true,
      internal = false,
      arguments = Map("x-expires" -> oneMinute)),
      Exchange(requeueExchange,
        durable = false,
        autoDelete = true,
        internal = false,
        arguments = Map("x-expires" -> oneMinute)),
      Exchange(redeliverExchange,
      durable = false,
      autoDelete = true,
      internal = false,
      arguments = Map("x-expires" -> oneMinute)))

    val bindings = List(Binding(redeliverExchange, mainQueue, RoutingKey(name), Map.empty),
    Binding(requeueExchange, requeueQueue, RoutingKey(name), Map.empty),
    Binding(deadletterExchange, deadletterQueue, RoutingKey(name), Map.empty))

    val configuration: List[Declaration] = queues ++ exchanges ++ bindings

    for {
      client <- amqpClientConfig
      result = Declaration.applyAll(configuration, client)
      _ = Await.result(result, 5.seconds)
    }
      yield (MessageQueue(mainQueue.value, rmqAdminConfig),
        MessageQueue(requeueQueue.value, rmqAdminConfig),
        MessageQueue(deadletterQueue.value, rmqAdminConfig))
  }


}
