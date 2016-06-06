package itv.bucky

import com.rabbitmq.client.{AMQP, MessageProperties}
import itv.bucky.AmqpClient._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.{Blob, BlobMarshaller}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.concurrent.Eventually
import Eventually._
import scala.collection.JavaConverters
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import JavaConverters._

class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  import DeserializerResult._
  private val published = ()

  val messageDeserializer = new BlobDeserializer[String] {
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
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    for {
      amqpClient <- amqpClientConfig
      queues <- IntegrationUtils.declareQueue(testQueueName)
      (_, testRequeue, _) = queues
      publish <- amqpClient.publisher()
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), AlwaysRequeue,requeuePolicy, messageDeserializer)
    } {
      val body = Blob.from("Hello World!")

      publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).futureValue shouldBe published

      eventually {
        testRequeue.allMessages.map(_.payload) shouldBe List(body)
      }(requeuePatienceConfig)
    }
  }

  private def getHeader(header: String, properties: AMQP.BasicProperties): Option[String] =
    properties.getHeaders.asScala.get(header).map(_.toString)

  test("Should retain any custom headers when republishing") {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    for {
      amqpClient <- amqpClientConfig
      _ <- IntegrationUtils.declareQueue(testQueueName)
      publish <- amqpClient.publisher()
      stubHandler = new StubRequeueHandler[Delivery]
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)

      val headers: java.util.Map[String, AnyRef] = Map[String, AnyRef]("foo" -> "bar").asJava
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().headers(headers).build()
      publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), properties, Blob.from("Hello World!"))).futureValue shouldBe published

      eventually {
        val headersOfReceived = stubHandler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }(requeuePatienceConfig)
    }
  }

    test("Should retain any custom amqp properties when republishing") {
      val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

      val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

      for {
        amqpClient <- amqpClientConfig
        _ <- IntegrationUtils.declareQueue(testQueueName)
        publish <- amqpClient.publisher()
        stubHandler = new StubRequeueHandler[Delivery]
        consumer <- requeueOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
      } {
        stubHandler.nextResponse = Future.successful(Requeue)

        val expectedCorrelationId: String = "banana"
        val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().correlationId(expectedCorrelationId).build()
        publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), properties, Blob.from("Hello World!"))).futureValue shouldBe published

        eventually {
          stubHandler.receivedMessages.count(_.properties.getCorrelationId == expectedCorrelationId) should be > 1
        }(requeuePatienceConfig)
      }
    }

    test("It should not requeue when the handler Acks") {
      val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

      val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

      for {
        amqpClient <- amqpClientConfig
        queues <- IntegrationUtils.declareQueue(testQueueName)
        (testQueue, testRequeue, _) = queues
        publish <- amqpClient.publisher()
        stubHandler = new StubRequeueHandler[Int]
        consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy, intMessageDeserializer)
      } {
        stubHandler.nextResponse = Future.successful(Consume(Ack))
        publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe published

        eventually {
          testQueue.allMessages shouldBe 'empty
        }
        testRequeue.allMessages shouldBe 'empty
        stubHandler.receivedMessages.length shouldBe 1
      }
    }

    test("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
      val testQueueName = "bucky-requeue-consumer-test" + Random.nextInt()
      val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

      for {
        amqpClient <- amqpClientConfig
        _ <- IntegrationUtils.declareQueue(testQueueName)
        publish <- amqpClient.publisher()
        stubHandler = new StubRequeueHandler[Int]
        consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy, intMessageDeserializer)
      } {
        stubHandler.nextResponse = Future.successful(Requeue)
        publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe published

        eventually {
          stubHandler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
        }(requeuePatienceConfig)
      }
    }

    test("It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
      val testQueueName = "bucky-requeue-consumer-test" + Random.nextInt()
      val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

      for {
        amqpClient <- amqpClientConfig
        queues <- IntegrationUtils.declareQueue(testQueueName)
        (testQueue, testRequeue, testDeadletterQueue) = queues
        publish <- amqpClient.publisher()
        stubHandler = new StubRequeueHandler[Int]
        consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy, intMessageDeserializer)
      } {
        stubHandler.nextResponse = Future.successful(Requeue)
        val payload = Blob.from(1)
        publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, payload)).futureValue shouldBe published

        eventually {
          testQueue.allMessages shouldBe 'empty
          testRequeue.allMessages shouldBe 'empty
          stubHandler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
          testDeadletterQueue.allMessages.map(_.payload) shouldBe List(payload)
        }(requeuePatienceConfig)
      }
    }

    test("It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
      val testQueueName = "bucky-requeue-consumer-test" + Random.nextInt()
      val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

      val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1)

      println(negativeProcessAttemptsRequeuePolicy)

      for {
        amqpClient <- amqpClientConfig
        queues <- IntegrationUtils.declareQueue(testQueueName)
        (testQueue, testRequeue, testDeadletterQueue) = queues
        publish <- amqpClient.publisher()
        stubHandler = new StubRequeueHandler[Int]
        consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)
      } {
        stubHandler.nextResponse = Future.successful(Requeue)
        val payload = Blob.from(1)
        publish(PublishCommand(ExchangeName(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, payload)).futureValue shouldBe published

        eventually {
          testQueue.allMessages shouldBe 'empty
          testRequeue.allMessages shouldBe 'empty
          stubHandler.receivedMessages.size shouldBe 1
          testDeadletterQueue.allMessages.map(_.payload) shouldBe List(payload)
        }(requeuePatienceConfig)
      }
    }
}


object AlwaysRequeue extends RequeueHandler[String] {

  override def apply(message: String): Future[RequeueConsumeAction] = {
    Future.successful(Requeue)
  }

}


