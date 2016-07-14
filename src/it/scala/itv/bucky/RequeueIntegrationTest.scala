package itv.bucky

import com.rabbitmq.client.{AMQP, MessageProperties}
import itv.bucky.pattern.requeue._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.concurrent.Eventually
import Eventually._
import com.rabbitmq.client.AMQP.BasicProperties
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller

import scala.collection.JavaConverters
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import JavaConverters._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.rmq.MessageQueue
import itv.bucky.Unmarshaller._

class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  private val published = ()

  implicit val intMessageDeserializer = StringPayloadUnmarshaller.map(_.toInt)

  implicit val intMarshaller: PayloadMarshaller[Int] = StringPayloadMarshaller.contramap(_.toString)

  val requeuePatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 5.second, interval = 100.millis)

  val exchange = ExchangeName("")

  val requeuePolicy = RequeuePolicy(3)

  ignore("Should retain any custom headers when republishing") {
    val handler = new StubRequeueHandler[Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val headers: java.util.Map[String, AnyRef] = Map[String, AnyRef]("foo" -> "bar").asJava
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().headers(headers).build()

      app.publish(Payload.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        val headersOfReceived = handler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }(requeuePatienceConfig)
    }
  }

  ignore("Should retain any custom amqp properties when republishing") {
    val handler = new StubRequeueHandler[Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val expectedCorrelationId: String = "banana"
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().correlationId(expectedCorrelationId).build()
      app.publish(Payload.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        handler.receivedMessages.count(_.properties.getCorrelationId == expectedCorrelationId) should be > 1
      }(requeuePatienceConfig)

    }
  }

  ignore("It should not requeue when the handler Acks") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Ack)

      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
      }
      app.requeue.allMessages shouldBe 'empty
      handler.receivedMessages.length shouldBe 1

    }
  }

  ignore("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  ignore("It should requeue the message if handler returns a failed future and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.failed(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  ignore("(Raw) requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextException = Some(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  ignore("Requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextException = Some(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  ignore("It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      val payload = Payload.from(1)
      app.publish(payload).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
        app.requeue.allMessages shouldBe 'empty
        handler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
        app.deadletterQueue.allMessages.map(b => Payload(b.payload.content)) shouldBe List(payload)
      }(requeuePatienceConfig)
    }
  }

  ignore("It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
    val handler = new StubRequeueHandler[Int]
    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1)
    Lifecycle.using(testLifecycle(handler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)) { app =>

      handler.nextResponse = Future.successful(Requeue)
      val payload = Payload.from(1)
      app.publish(payload).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
        app.requeue.allMessages shouldBe 'empty
        handler.receivedMessages.size shouldBe 1
        app.deadletterQueue.allMessages.map(b => Payload(b.payload.content)) shouldBe List(payload)
      }(requeuePatienceConfig)
    }
  }

  case class TestFixture(queue: MessageQueue, requeue: MessageQueue, deadletterQueue: MessageQueue, publisher: Publisher[PublishCommand]) {
    def publish(body: Payload, properties: BasicProperties = MessageProperties.MINIMAL_PERSISTENT_BASIC): Future[Unit] = publisher(
      PublishCommand(ExchangeName(""), RoutingKey(queue.name), properties, body))
  }


  case class BaseTextFixture(amqpClient: AmqpClient, queueName: QueueName, testFixture: TestFixture)

  private def baseTestLifecycle(): Lifecycle[BaseTextFixture] = {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    for {
      amqpClient <- amqpClientConfig
      queues <- IntegrationUtils.declareRequeueQueues(testQueueName)
      (testQueue, testRequeue, testDeadletterQueue) = queues
      publish <- amqpClient.publisher()
    } yield BaseTextFixture(amqpClient, QueueName(testQueueName), TestFixture(testQueue, testRequeue, testDeadletterQueue, publish))
  }

  def testLifecycle[T](handler: RequeueHandler[T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T]): Lifecycle[TestFixture] = for {
    base <- baseTestLifecycle()
    consumer <- base.amqpClient.requeueHandlerOf(base.queueName, handler, requeuePolicy, unmarshaller)
  } yield base.testFixture


  def testLifecycle(handler: RequeueHandler[Delivery], requeuePolicy: RequeuePolicy): Lifecycle[TestFixture] = for {
    base <- baseTestLifecycle()
    consumer <- base.amqpClient.requeueOf(base.queueName, handler, requeuePolicy)
  } yield base.testFixture


  private def getHeader(header: String, properties: AMQP.BasicProperties): Option[String] =
    properties.getHeaders.asScala.get(header).map(_.toString)

}

object AlwaysRequeue extends RequeueHandler[String] {
  override def apply(message: String): Future[RequeueConsumeAction] =
    Future.successful(Requeue)
}


