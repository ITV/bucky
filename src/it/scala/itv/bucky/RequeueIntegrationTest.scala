package itv.bucky

import com.rabbitmq.client.MessageProperties
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
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scalaz.Id

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

  test("Can publish messages to a (pre-existing) queue") {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    val (_, testRequeue, _) = IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName))(AlwaysRequeue)
    } {
      val body = Blob.from("Hello World!")
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).futureValue shouldBe (())

      eventually {
        testRequeue.getNextMessage().payload shouldBe body
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
      handler = new StubHandler[Int]
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName))(handler)
    } {
      handler.nextResponse = Future.successful(Ack)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe (())

      eventually {
        testQueue.allMessages shouldBe 'empty
      }
      testRequeue.allMessages shouldBe 'empty
      handler.receivedMessages.length shouldBe 1
    }
  }

  test("It should reprocess the message multiple times upon requeue") {
    val testQueueName = "bucky-requeue-consumer-test" + Random.nextInt()
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp
    IntegrationUtils.declareQueue(testQueueName)

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      handler = new StubHandler[Int]
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName))(handler)
    } {
      handler.nextResponse = Future.successful(Requeue)
      publish(PublishCommand(Exchange(""), RoutingKey(testQueueName), MessageProperties.MINIMAL_PERSISTENT_BASIC, Blob.from(1))).futureValue shouldBe (())

      eventually {
        handler.receivedMessages.length should be > 3
      }(requeuePatienceConfig)
    }
  }

}


object AlwaysRequeue extends Handler[String] {

  override def apply(message: String): Future[ConsumeAction] = {
    Future.successful(Requeue)
  }

}


