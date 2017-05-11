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

  case class TestFixture(publisher: Publisher[Task, PublishCommand], routingKey: RoutingKey, exchangeName: ExchangeName, queueName: QueueName, amqpClient: TaskAmqpClient, requeueQueueName: QueueName, dlqHandler: Option[StubConsumeHandler[Task, Delivery]] = None) {
    def publish(body: Payload, properties: MessageProperties = MessageProperties.persistentBasic): Task[Unit] = publisher(
      PublishCommand(exchangeName, RoutingKey(queueName.value), properties, body))
  }


  sealed trait RequeueStrategy

  case object NoneHandler extends RequeueStrategy

  case class NoneRequeue(handler: Handler[Task, Delivery]) extends RequeueStrategy

  case class SimpleRequeue(handler: Handler[Task, Delivery]) extends RequeueStrategy

  case class RawRequeue(handler: RequeueHandler[Task, Delivery], requeuePolicy: RequeuePolicy) extends RequeueStrategy

  case class TypeRequeue[T](handler: RequeueHandler[Task, T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T]) extends RequeueStrategy


  def withPublisher(testQueueName: QueueName = randomQueue(), requeueStrategy: RequeueStrategy = NoneHandler, shouldDeclare: Boolean = true)(f: TestFixture => Unit): Unit = {
    val routingKey = RoutingKey(testQueueName.value)

    val exchange = ExchangeName("")

    val amqpClient = TaskAmqpClient(IntegrationUtils.config)

    val declaration = requeueStrategy match {
      case NoneRequeue(_) => defaultDeclaration(testQueueName)
      case SimpleRequeue(_) => basicRequeueDeclarations(testQueueName, retryAfter = 1.second) collect {
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
    f(TestFixture(publisher, routingKey, exchange, testQueueName, amqpClient, QueueName(s"${testQueueName.value}.requeue")))

    logger.info(s"Closing the the publisher")
    Channel.closeAll(amqpClient.channel)

  }


  def randomPayload() =
    Payload.from(randomString())


  def randomString() =
    s"Hello World ${new Random().nextInt(10000)}! "


  def randomQueue() =
    QueueName(s"bucky-queue-${new Random().nextInt(10000)}")


  def getHeader(header: String, properties: MessageProperties): Option[String] =
    properties.headers.get(header).map(_.toString)


  def withPublisherAndConsumer(queueName: QueueName = randomQueue(),
                               requeueStrategy: RequeueStrategy)(f: TestFixture => Unit): Unit = {
    withPublisher(queueName, requeueStrategy = requeueStrategy) { app =>

      import scalaz.stream.Process
      val consumer: Process[Task, Unit] = requeueStrategy match {
        case NoneHandler => Process.empty[Task, Unit]
        case RawRequeue(requeueHandler, requeuePolicy) =>
          app.amqpClient.requeueOf(app.queueName, requeueHandler, requeuePolicy)
        case TypeRequeue(requeueHandler, requeuePolicy, unmarshaller) =>
          app.amqpClient.requeueHandlerOf(app.queueName, requeueHandler, requeuePolicy, unmarshaller)
        case SimpleRequeue(handler) => app.amqpClient.consumer(app.queueName, handler)
        case NoneRequeue(handler) => app.amqpClient.consumer(app.queueName, handler)
      }

      consumer.run.unsafePerformAsync { result =>
        logger.info(s"Closing consumer ${app.queueName}: $result")
      }

      val dlqHandler = requeueStrategy match {
        case NoneHandler => None
        case NoneRequeue(_) => None
        case _ =>
          val dlqHandler = new StubConsumeHandler[Task, Delivery]
          val dlqQueueName = QueueName(s"${queueName.value}.dlq")
          app.amqpClient.consumer(dlqQueueName, dlqHandler).run.unsafePerformAsync { result =>
            logger.info(s"Closing dead letter consumer $dlqQueueName}: $result")
          }

          Some(dlqHandler)
      }

      f(app.copy(dlqHandler = dlqHandler))

      logger.info(s"Closing the the consumer")
    }
  }


}
