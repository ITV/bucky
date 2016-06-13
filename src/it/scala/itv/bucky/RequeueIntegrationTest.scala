package itv.bucky

import com.rabbitmq.client.{AMQP, MessageProperties}
import itv.bucky.pattern.requeue._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.{Blob, BlobMarshaller}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.concurrent.Eventually
import Eventually._
import com.rabbitmq.client.AMQP.BasicProperties

import scala.collection.JavaConverters
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import JavaConverters._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.rmq.MessageQueue

class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  import DeserializerResult._

  private val published = ()

  val stringMessageDeserializer = new BlobDeserializer[String] {
    override def apply(blob: Blob): DeserializerResult[String] = blob.to[String].success
  }

  implicit val intMessageDeserializer = new BlobDeserializer[Int] {
    override def apply(blob: Blob): DeserializerResult[Int] = blob.to[String].toInt.success
  }

  implicit val intBlobMarshaller: BlobMarshaller[Int] = BlobMarshaller[Int] { i: Int => Blob(i.toString.getBytes("UTF-8")) }

  val requeuePatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 5.second, interval = 100.millis)

  val exchange = ExchangeName("")

  val requeuePolicy = RequeuePolicy(3)

  test("Can publish messages to a (pre-existing) queue") {
    Lifecycle.using(testLifecycle(AlwaysRequeue, requeuePolicy, stringMessageDeserializer)) { app =>
      val body = Blob.from("Hello World!")
      app.publish(body).futureValue shouldBe published

      eventually {
        app.requeue.allMessages.map(_.payload) shouldBe List(body)
      }(requeuePatienceConfig)
    }
  }

  test("Should retain any custom headers when republishing") {
    val handler = new StubRequeueHandler[Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val headers: java.util.Map[String, AnyRef] = Map[String, AnyRef]("foo" -> "bar").asJava
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().headers(headers).build()

      app.publish(Blob.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        val headersOfReceived = handler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }(requeuePatienceConfig)
    }
  }

  test("Should retain any custom amqp properties when republishing") {
    val handler = new StubRequeueHandler[Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val expectedCorrelationId: String = "banana"
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().correlationId(expectedCorrelationId).build()
      app.publish(Blob.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        handler.receivedMessages.count(_.properties.getCorrelationId == expectedCorrelationId) should be > 1
      }(requeuePatienceConfig)

    }
  }

  test("It should not requeue when the handler Acks") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Ack)

      app.publish(Blob.from(1)).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
      }
      app.requeue.allMessages shouldBe 'empty
      handler.receivedMessages.length shouldBe 1

    }
  }

  test("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      app.publish(Blob.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  test("It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
    val handler = new StubRequeueHandler[Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      val payload = Blob.from(1)
      app.publish(Blob.from(1)).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
        app.requeue.allMessages shouldBe 'empty
        handler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
        app.deadletterQueue.allMessages.map(_.payload) shouldBe List(payload)
      }(requeuePatienceConfig)
    }
  }

  test("It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
    val handler = new StubRequeueHandler[Int]
    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1)
    Lifecycle.using(testLifecycle(handler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)) { app =>

      handler.nextResponse = Future.successful(Requeue)
      val payload = Blob.from(1)
      app.publish(payload).futureValue shouldBe published

      eventually {
        app.queue.allMessages shouldBe 'empty
        app.requeue.allMessages shouldBe 'empty
        handler.receivedMessages.size shouldBe 1
        app.deadletterQueue.allMessages.map(_.payload) shouldBe List(payload)
      }(requeuePatienceConfig)
    }
  }

  case class TestFixture(queue: MessageQueue, requeue: MessageQueue, deadletterQueue: MessageQueue, publisher: Publisher[PublishCommand]) {
    def publish(body: Blob, properties: BasicProperties = MessageProperties.MINIMAL_PERSISTENT_BASIC): Future[Unit] = publisher(
      PublishCommand(ExchangeName(""), RoutingKey(queue.name), properties, body))
  }


  case class BaseTextFixture(amqpClient: AmqpClient, queueName: QueueName, testFixture: TestFixture)

  private def baseTestLifecycle(): Lifecycle[BaseTextFixture] = {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    for {
      amqpClient <- amqpClientConfig
      queues <- IntegrationUtils.declareQueue(testQueueName)
      (testQueue, testRequeue, testDeadletterQueue) = queues
      publish <- amqpClient.publisher()
    } yield BaseTextFixture(amqpClient, QueueName(testQueueName), TestFixture(testQueue, testRequeue, testDeadletterQueue, publish))
  }

  def testLifecycle[T](handler: RequeueHandler[T], requeuePolicy: RequeuePolicy, messageDeserializer: BlobDeserializer[T]): Lifecycle[TestFixture] = for {
    base <- baseTestLifecycle()
    consumer <- base.amqpClient.requeueHandlerOf(base.queueName, handler, requeuePolicy, messageDeserializer)
  } yield base.testFixture


  def testLifecycle[T](handler: RequeueHandler[Delivery], requeuePolicy: RequeuePolicy): Lifecycle[TestFixture] = for {
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


