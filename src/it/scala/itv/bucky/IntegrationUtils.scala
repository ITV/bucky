package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.bucky.decl._
import itv.bucky.pattern.requeue._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.rmq._
import itv.httpyroraptor._

import scala.concurrent.duration._
import scalaz._
import Scalaz._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

object IntegrationUtils {




  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))


  def declareQueues(testQueueNames: QueueName*): (Seq[MessageQueue], AmqpClientConfig, HttpClient[Id]) = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHhttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    val queues = for {
      client <- amqpClientConfig
      declarations = testQueueNames.map(qn => Queue(qn).autoDelete.expires(2.minutes))
      _ <- DeclarationLifecycle(declarations, client)
    }
      yield testQueueNames.map(qn => MessageQueue(qn.value, rmqAdminConfig))

    Lifecycle.using(queues) { queues =>
      (queues, amqpClientConfig, rmqAdminHhttp)
    }
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

  def declareRequeueQueues(name: String): Lifecycle[(MessageQueue, MessageQueue, MessageQueue)] = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig, rmqAdminHttp: AuthenticatedHttpClient[Id.Id]) = configAndHttp

    val declarations = requeueDeclarations(QueueName(name), retryAfter = 1.second) collect {
      case ex: Exchange => ex.autoDelete.expires(1.minute)
      case q: Queue => q.autoDelete.expires(1.minute)
    }

    for {
      client <- amqpClientConfig
      _ <- DeclarationLifecycle(declarations, client)
    }
      yield (MessageQueue(name, rmqAdminConfig),
        MessageQueue(s"$name.requeue", rmqAdminConfig),
        MessageQueue(s"$name.dlq", rmqAdminConfig))
  }


}
