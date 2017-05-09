package com.itv.bucky.stream

import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Random
import scalaz.concurrent.Task


object IntegrationUtils extends StrictLogging {
  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

  def config: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")

    AmqpClientConfig(config.getString("rmq.host"), config.getInt("rmq.port"), config.getString("rmq.username"), config.getString("rmq.password"))
  }

  case class TestFixture(publish: Publisher[Task, PublishCommand], routingKey: RoutingKey, exchangeName: ExchangeName, queueName: QueueName, amqpClient: StreamAmqpClient, dlqHandler: Option[StubConsumeHandler[Task, Delivery]] = None)


  def withPublisher(testQueueName: QueueName = randomQueue(), shouldDeadLetter: Boolean = false, shouldDeclare: Boolean = true)(f: TestFixture => Unit): Unit = {
    val routingKey = RoutingKey(testQueueName.value)

    val exchange = ExchangeName("")

    val amqpClient = StreamAmqpClient(IntegrationUtils.config)

    val declaration = if (shouldDeadLetter)
      basicRequeueDeclarations(testQueueName, retryAfter = 1.second) collect {
        case ex: Exchange => ex.autoDelete.expires(1.minute)
        case q: Queue => q.autoDelete.expires(1.minute)
      }
    else
      defaultDeclaration(testQueueName)


    if (shouldDeclare)
      DeclarationExecutor(declaration, amqpClient, 5.seconds)

    val publisher: Publisher[Task, PublishCommand] = amqpClient.publisher()
    f(TestFixture(publisher, routingKey, exchange, testQueueName, amqpClient))

    IdChannel.closeAll(amqpClient.channel)

  }


  def randomPayload() =
    Payload.from(randomString())


  def randomString() =
    s"Hello World ${new Random().nextInt(10000)}! "


  def randomQueue() =
    QueueName(s"bucky-queue-${new Random().nextInt(10000)}")


  def withPublisherAndConsumer(handler: Handler[Task, Delivery], queueName: QueueName = randomQueue(), shouldDeadLetter: Boolean = false)(f: TestFixture => Unit): Unit = {
    withPublisher(queueName, shouldDeadLetter = shouldDeadLetter) { app =>
      app.amqpClient.consumer(app.queueName, handler).unsafePerformAsync { result =>
        logger.info(s"Closing consumer ${app.queueName}: $result")
      }

      val dlqHandler =
        if (shouldDeadLetter) {
          val dlqHandler = new StubConsumeHandler[Task, Delivery]
          val dlqQueueName = QueueName(s"${queueName.value}.dlq")
          app.amqpClient.consumer(dlqQueueName, dlqHandler).unsafePerformAsync { result =>
            logger.info(s"Closing consumer for dlq $dlqQueueName: $result")
          }
          Some(dlqHandler)
        } else None

      f(app.copy(dlqHandler = dlqHandler))
    }
  }


}
