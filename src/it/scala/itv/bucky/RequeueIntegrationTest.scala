package itv.bucky

import com.rabbitmq.client.{AMQP, MessageProperties}
import itv.bucky.AmqpClient._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.contentdelivery.testutilities.rmq.{MessageQueue, BrokerConfig}
import itv.httpyroraptor.AuthenticatedHttpClient
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
import scalaz.Id
import JavaConverters._

class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  import BlobSerializer._
  import DeserializerResult._

  implicit val messageDeserializer = new BlobDeserializer[String] {
    override def apply(blob: Blob): DeserializerResult[String] = blob.to[String].success
  }

  implicit val intMessageDeserializer = new BlobDeserializer[Int] {
    override def apply(blob: Blob): DeserializerResult[Int] = blob.to[String].toInt.success
  }

  implicit val intBlobMarshaller: BlobMarshaller[Int] = BlobMarshaller[Int] { i: Int => Blob(i.toString.getBytes("UTF-8")) }

  val requeuePatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 5.second, interval = 100.millis)

  val exchange = Exchange("")

  val requeuePolicy = RequeuePolicy(3)

  test("Can publish messages to a (pre-existing) queue") {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    val (_, testRequeue, _) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), AlwaysRequeue, requeuePolicy)
    } {
      val body = Blob.from("Hello World!")
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).futureValue shouldBe (())

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
    val (_, testRequeue, _) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Delivery]
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)

      val headers: java.util.Map[String, AnyRef] = Map[String, AnyRef]("foo" -> "bar").asJava
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().headers(headers).build()
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), properties, Blob.from("Hello World!"))).futureValue shouldBe (())

      eventually {
        val headersOfReceived = stubHandler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }(requeuePatienceConfig)
    }
  }

  test("Should retain any custom amqp properties when republishing") {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    val (_, testRequeue, _) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Delivery]
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)

      val expectedCorrelationId: String = "banana"
      val properties = MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().correlationId(expectedCorrelationId).build()
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), properties, Blob.from("Hello World!"))).futureValue shouldBe (())

      eventually {
        stubHandler.receivedMessages.count(_.properties.getCorrelationId == expectedCorrelationId) should be > 1
      }(requeuePatienceConfig)
    }
  }

  test("It should not requeue when the handler Acks") {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    val (testQueue, testRequeue, _) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Int]
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Ack)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe (())

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
    IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Int]
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe (())

      eventually {
        stubHandler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }(requeuePatienceConfig)
    }
  }

  test("It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
    val testQueueName = "bucky-requeue-consumer-test" + Random.nextInt()
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    val (testQueue, testRequeue, testDeadletterQueue) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Int]
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, requeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)
      val payload = Blob.from(1)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, payload)).futureValue shouldBe (())

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
    val (testQueue, testRequeue, testDeadletterQueue) = IntegrationUtils.declareQueue(testQueueName)

    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1)

    println(negativeProcessAttemptsRequeuePolicy)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      stubHandler = new StubHandler[Int]
      consumer <- requeueHandlerOf(amqpClient)(QueueName(testQueueName), stubHandler, negativeProcessAttemptsRequeuePolicy)
    } {
      stubHandler.nextResponse = Future.successful(Requeue)
      val payload = Blob.from(1)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, payload)).futureValue shouldBe (())

      eventually {
        testQueue.allMessages shouldBe 'empty
        testRequeue.allMessages shouldBe 'empty
        stubHandler.receivedMessages.size shouldBe 1
        testDeadletterQueue.allMessages.map(_.payload) shouldBe List(payload)
      }(requeuePatienceConfig)
    }
  }

}


object AlwaysRequeue extends Handler[String] {

  override def apply(message: String): Future[ConsumeAction] = {
    Future.successful(Requeue)
  }

}


