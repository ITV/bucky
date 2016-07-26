package itv.bucky

import com.typesafe.config.ConfigFactory
import itv.bucky.decl._
import itv.bucky.pattern.requeue._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.rmq._

import scala.concurrent.duration._

object IntegrationUtils {

  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

  def declareQueues(testQueueNames: QueueName*): (Seq[MessageQueue], AmqpClientConfig) = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig) = configAndHttp

    val queues = for {
      client <- amqpClientConfig
      declarations = testQueueNames.map(qn => Queue(qn).autoDelete.expires(2.minutes))
      _ <- DeclarationLifecycle(declarations, client)
    }
      yield testQueueNames.map(qn => MessageQueue(qn.value, rmqAdminConfig))

    Lifecycle.using(queues) { queues =>
      (queues, amqpClientConfig)
    }
  }

  def configAndHttp: (AmqpClientConfig, BrokerConfig) = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    val amqpClientConfig = AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
    val adminConfig = config.getConfig("rmq.admin-api")
    val rmqAdminConfig = BrokerConfig(adminConfig.getString("username"), adminConfig.getString("password"), host, adminConfig.getInt("port"))

    (amqpClientConfig, rmqAdminConfig)
  }

  def declareRequeueQueues(name: String): Lifecycle[(MessageQueue, MessageQueue, MessageQueue)] = {
    val (amqpClientConfig: AmqpClientConfig, rmqAdminConfig: BrokerConfig) = configAndHttp

    val declarations = basicRequeueDeclarations(QueueName(name), retryAfter = 1.second) collect {
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
