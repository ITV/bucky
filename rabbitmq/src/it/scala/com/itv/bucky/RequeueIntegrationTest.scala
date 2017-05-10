package com.itv.bucky

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.SameThreadExecutionContext.implicitly
import com.itv.bucky.Unmarshaller._
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.pattern.requeue._
import com.itv.bucky.lifecycle._
import com.itv.bucky.future._
import com.itv.lifecycle.Lifecycle
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Random, Success}


class RequeueIntegrationTest extends FunSuite with ScalaFutures {

  private val published = ()

  implicit val intMessageDeserializer = StringPayloadUnmarshaller.map(_.toInt)

  implicit val intMarshaller: PayloadMarshaller[Int] = StringPayloadMarshaller.contramap(_.toString)

  implicit val requeuePatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 60.second, interval = 100.millis)

  val exchange = ExchangeName("")

  val requeuePolicy = RequeuePolicy(3, 1.second)

  test("Should retain any custom headers when republishing") {
    val handler = new StubRequeueHandler[Future, Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val properties = MessageProperties.persistentTextPlain.withHeader("foo" -> "bar")

      app.publish(Payload.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        val headersOfReceived = handler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }
    }
  }

  test("Should retain any custom amqp properties when republishing") {
    val handler = new StubRequeueHandler[Future, Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextResponse = Future.successful(Requeue)

      val expectedCorrelationId: Option[String] = Some("banana")
      val properties = MessageProperties.persistentTextPlain.copy(correlationId = expectedCorrelationId)
      app.publish(Payload.from("Hello World!"), properties).futureValue shouldBe published

      eventually {
        handler.receivedMessages.count(_.properties.correlationId == expectedCorrelationId) should be > 1
      }

    }
  }

  test("It should not requeue when the handler Acks") {
    val handler = new StubRequeueHandler[Future, Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Ack)

      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
      }
      app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
      app.deadletterQueue.receivedMessages shouldBe 'empty
      handler.receivedMessages.length shouldBe 1
    }
  }

  test("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
    val handler = new StubRequeueHandler[Future, Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("It should requeue the message if handler returns a failed future and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Future, Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.failed(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("(Raw) requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Future, Delivery]
    Lifecycle.using(testLifecycle(handler, requeuePolicy)) { app =>
      handler.nextException = Some(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("Requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Future, Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextException = Some(new RuntimeException("Handler problem"))
      app.publish(Payload.from(1)).futureValue shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
    val handler = new StubRequeueHandler[Future, Int]
    Lifecycle.using(testLifecycle(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Future.successful(Requeue)
      val payload = Payload.from(1)
      app.publish(payload).futureValue shouldBe published

      eventually {
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        handler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
        app.deadletterQueue.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }

  test("It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
    val handler = new StubRequeueHandler[Future,Int]
    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1, 1.second)
    Lifecycle.using(testLifecycle(handler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)) { app =>

      handler.nextResponse = Future.successful(Requeue)
      val payload = Payload.from(1)
      app.publish(payload).futureValue shouldBe published

      eventually {
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        handler.receivedMessages.size shouldBe 1
        app.deadletterQueue.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }

  case class TestFixture(amqpClient: AmqpClient[Lifecycle, Future, Throwable, Unit], queueName: QueueName, requeueQueueName: QueueName, deadletterQueue: QueueWatcher[Delivery], publisher: Publisher[Future, PublishCommand]) {
    def publish(body: Payload, properties: MessageProperties = MessageProperties.persistentBasic): Future[Unit] = publisher(
      PublishCommand(ExchangeName(""), RoutingKey(queueName.value), properties, body))
  }

  private def baseTestLifecycle(): Lifecycle[TestFixture] = {
    val testQueueName = "bucky-requeue-consumer-ack" + Random.nextInt()

    val declarations = requeueDeclarations(QueueName(testQueueName), RoutingKey(testQueueName), Exchange(ExchangeName(testQueueName + ".dlx")), retryAfter = 1.second) collect {
      case ex: Exchange => ex.autoDelete.expires(1.minute)
      case q: Queue => q.autoDelete.expires(1.minute)
    }

    for {
      client <- AmqpClientLifecycle(IntegrationUtils.config)
      _ <- DeclarationLifecycle(declarations, client)

      publish <- client.publisher()
      deadletterWatcher = new QueueWatcher[Delivery]
      deadletterQueue <- client.consumer(QueueName(testQueueName + ".dlq"), deadletterWatcher)
    } yield TestFixture(client, QueueName(testQueueName), QueueName(testQueueName + ".requeue"), deadletterWatcher, publish)
  }

  def testLifecycle[T](handler: RequeueHandler[Future, T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T])(implicit M: Monad[Future]): Lifecycle[TestFixture] = for {
    testFixture <- baseTestLifecycle()
    consumer <- testFixture.amqpClient.requeueHandlerOf(testFixture.queueName, handler, requeuePolicy, unmarshaller)
  } yield testFixture


  def testLifecycle(handler: RequeueHandler[Future, Delivery], requeuePolicy: RequeuePolicy): Lifecycle[TestFixture] = for {
    testFixture <- baseTestLifecycle()
    consumer <- testFixture.amqpClient.requeueOf(testFixture.queueName, handler, requeuePolicy)
  } yield testFixture


  private def getHeader(header: String, properties: MessageProperties): Option[String] =
    properties.headers.get(header).map(_.toString)

}

object FutureAlwaysRequeue extends AlwaysRequeue[Future, Throwable]

