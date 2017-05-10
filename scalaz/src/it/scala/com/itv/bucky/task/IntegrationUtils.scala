package com.itv.bucky.task

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

    AmqpClientConfig(config.getString("rmq.host"), config.getInt("rmq.port"), config.getString("rmq.username"), config.getString("rmq.password"))
  }

  case class TestFixture(publish: Publisher[Task, PublishCommand], routingKey: RoutingKey, exchangeName: ExchangeName, queueName: QueueName, amqpClient: TaskAmqpClient, dlqHandler: Option[StubConsumeHandler[Task, Delivery]] = None)


  sealed trait RequeueStrategy

  case object NoneRequeue extends RequeueStrategy

  case object SimpleRequeue extends RequeueStrategy

  case class DeliveryRequeue(handler: RequeueHandler[Task, Delivery], requeuePolicy: RequeuePolicy) extends RequeueStrategy

  case class TypeRequeue[T](handler: RequeueHandler[Task, T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T]) extends RequeueStrategy


  def withPublisher(testQueueName: QueueName = randomQueue(), requeueStrategy: RequeueStrategy = NoneRequeue, shouldDeclare: Boolean = true)(f: TestFixture => Unit): Unit = {
    val routingKey = RoutingKey(testQueueName.value)

    val exchange = ExchangeName("")

    val amqpClient = TaskAmqpClient(IntegrationUtils.config)

    val declaration = requeueStrategy match {
      case NoneRequeue => defaultDeclaration(testQueueName)
      case SimpleRequeue => basicRequeueDeclarations(testQueueName, retryAfter = 1.second) collect {
        case ex: Exchange => ex.autoDelete.expires(1.minute)
        case q: Queue => q.autoDelete.expires(1.minute)
      }
      case _ => requeueDeclarations(testQueueName, RoutingKey(testQueueName.value), Exchange(ExchangeName(s"${testQueueName.value}.dlx")), retryAfter = 1.second) collect {
        case ex: Exchange => ex.autoDelete.expires(1.minute)
        case q: Queue => q.autoDelete.expires(1.minute)
      }

    }

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


  def withPublisherAndConsumer(handler: Handler[Task, Delivery],
                               queueName: QueueName = randomQueue(),
                               requeueStrategy: RequeueStrategy = NoneRequeue)(f: TestFixture => Unit): Unit = {
    withPublisher(queueName, requeueStrategy = requeueStrategy) { app =>

      val consumer: Task[Unit] = requeueStrategy match {
        case DeliveryRequeue(requeueHandler, requeuePolicy) =>
          app.amqpClient.requeueOf(app.queueName, requeueHandler, requeuePolicy)
        case TypeRequeue(requeueHandler, requeuePolicy, unmarshaller) =>
          app.amqpClient.requeueHandlerOf(app.queueName, requeueHandler, requeuePolicy, unmarshaller)
        case _ => app.amqpClient.consumer(app.queueName, handler)
      }

      consumer.unsafePerformAsync { result =>
        logger.info(s"Closing consumer ${app.queueName}: $result")
      }


      val dlqHandler = requeueStrategy match {
        case NoneRequeue => None
        case _ =>
          val dlqHandler = new StubConsumeHandler[Task, Delivery]
          val dlqQueueName = QueueName(s"${queueName.value}.dlq")
          app.amqpClient.consumer(dlqQueueName, dlqHandler).unsafePerformAsync { result =>
            logger.info(s"Closing consumer for dlq $dlqQueueName: $result")
          }
          Some(dlqHandler)
      }

      f(app.copy(dlqHandler = dlqHandler))
    }
  }


}
