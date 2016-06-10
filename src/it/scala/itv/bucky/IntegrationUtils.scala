package itv.bucky

import com.typesafe.config.ConfigFactory
<<<<<<< HEAD
import itv.bucky.decl.{Declaration, Exchange, Queue}
=======
import itv.bucky.decl.pattern.Pattern
import itv.bucky.decl.{Binding, Declaration, Exchange, Queue}
>>>>>>> Integration tests should use requeue pattern
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.contentdelivery.testutilities.rmq.{BrokerConfig, MessageQueue}
import itv.httpyroraptor._
import itv.utils.Blob
import org.scalatest.Matchers._

<<<<<<< HEAD
import scala.concurrent.Await
import scala.concurrent.duration._
=======
import scala.concurrent.{Await, Future}
>>>>>>> Integration tests should use requeue pattern
import scalaz.Id
import scalaz.Id.Id

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

    val declarations = Pattern.Requeue(QueueName(name), retryAfter = 1.second) collect {
      case ex: Exchange => ex.autoDelete.expires(1.minute)
      case q: Queue => q.autoDelete.expires(1.minute)
    }

    for {
      client <- amqpClientConfig
      result = Declaration.applyAll(declarations, client)
      _ = Await.result(result, 5.seconds)
    }
      yield (MessageQueue(name, rmqAdminConfig),
        MessageQueue(s"$name.requeue", rmqAdminConfig),
        MessageQueue(s"$name.dlq", rmqAdminConfig))
  }


}
