package itv.bucky

import com.rabbitmq.client.MessageProperties
import itv.bucky.AmqpClient._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.{Blob, BlobMarshaller}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  import BlobSerializer._
  import DeserializerResult._

  implicit val messageDeserializer = new BlobDeserializer[String] {
    override def apply(blob: Blob): DeserializerResult[String] = blob.to[String].success
  }

  implicit val intMessageDeserializer = new BlobDeserializer[Int] {
    override def apply(blob: Blob): DeserializerResult[Int] = blob.to[String].toInt.success
  }

  implicit val intBlobMarshaller: BlobMarshaller[Int] = BlobMarshaller[Int]{i : Int => Blob(i.toString.getBytes("UTF-8"))}

  val testQueueName = "bucky-requeue-consumer-test"
  val testRequeueName = testQueueName + ".requeue"
  val testDeadletterQueueName = testQueueName + ".dlq"
  val routingKey = RoutingKey(testQueueName)

  val exchange = Exchange("")


  test("Can publish messages to a (pre-existing) queue") {
    lazy val (Seq(testQueue, testRequeue), amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(testQueueName, testRequeueName)
    testQueue.purge()
    testRequeue.purge()

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName))(AlwaysRequeue)
    } {
      val body = Blob.from("Hello World!")
      publish(PublishCommand(Exchange(""), routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).futureValue shouldBe (())

      testRequeue.getNextMessage().payload shouldBe body
    }
  }

  test("It should requeue when the handler decides") {
    lazy val (Seq(testQueue, testRequeue, testDeadletterQueue), amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(testQueueName, testRequeueName, testDeadletterQueueName)
    IntegrationUtils.defineDeadlettering(testQueueName)
    testQueue.purge()
    testRequeue.purge()
    testDeadletterQueue.purge()

    for {
      amqpClient <- amqpClientConfig
      publish <- amqpClient.publisher()
      consumer <- requeueOf(amqpClient)(QueueName(testQueueName))(RequeueWhenRequires)

    } {
      val body = Blob.from(1)
      publish(PublishCommand(Exchange(""), routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, body)).futureValue shouldBe (())

      testRequeue.allMessages shouldBe 'empty

      val requeueBody = Blob.from(3)
      publish(PublishCommand(Exchange(""), routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, requeueBody)).futureValue shouldBe (())

      testRequeue.getNextMessage().payload shouldBe requeueBody

//      val nackBody = Blob.from("nack")
//      publish(PublishCommand(Exchange(""), routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC, nackBody)).futureValue shouldBe (())

//      testRequeue.allMessages shouldBe 'empty
//      testDeadletterQueue.getNextMessage()(Eventually.PatienceConfig(timeout = 2.second, interval = 100.millis)).payload shouldBe nackBody


    }
  }

  val another = blobSerializer[String] using RoutingKey(testQueueName) using Exchange("")
}


object AlwaysRequeue extends Handler[String] {

  override def apply(message: String): Future[ConsumeAction] = {
    Future.successful(Requeue)
  }

}

object RequeueWhenRequires extends Handler[Int] {

  override def apply(message: Int): Future[ConsumeAction] = message match {
    case 1 => Future.successful(Ack)
    case _ => Future.successful(Requeue)
  }

}


