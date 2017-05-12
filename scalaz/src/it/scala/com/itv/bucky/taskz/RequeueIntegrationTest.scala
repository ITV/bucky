package com.itv.bucky.taskz

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.taskz.IntegrationUtils._
import com.itv.bucky._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.{Random, Success}
import scalaz.\/-
import scalaz.concurrent.Task

class RequeueIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {

  private val published = \/-(())

  implicit val intMessageDeserializer = StringPayloadUnmarshaller.map(_.toInt)

  implicit val intMarshaller: PayloadMarshaller[Int] = StringPayloadMarshaller.contramap(_.toString)

  implicit val requeuePatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 60.second, interval = 100.millis)

  val exchange = ExchangeName("")

  val requeuePolicy = RequeuePolicy(3, 1.second)

  test(s"Should retain any custom headers when republishing") {
    val handler = new StubRequeueHandler[Task, Delivery]()
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      handler.nextResponse = Task.now(Requeue)
      val properties = MessageProperties.persistentTextPlain.withHeader("foo" -> "bar")

      app.publish(randomPayload(), properties).unsafePerformSyncAttempt shouldBe published

      eventually {
        val headersOfReceived = handler.receivedMessages.map(d => getHeader("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }
    }
  }

  test("Should retain any custom amqp properties when republishing") {
    val handler = new StubRequeueHandler[Task, Delivery]
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      handler.nextResponse = Task.now(Requeue)

      val expectedCorrelationId: Option[String] = Some("banana")
      val properties = MessageProperties.persistentTextPlain.copy(correlationId = expectedCorrelationId)
      app.publish(Payload.from("Hello World!"), properties).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.count(_.properties.correlationId == expectedCorrelationId) should be > 1
      }
    }
  }

  test("It should not requeue when the handler Acks") {
    val handler = new StubRequeueHandler[Task, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Task.now(Ack)

      app.publish(Payload.from(1)).unsafePerformSyncAttempt shouldBe published

      eventually {
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
      }
      app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
      app.dlqHandler.get.receivedMessages shouldBe 'empty
      handler.receivedMessages.length shouldBe 1
    }
  }

  test("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
    val handler = new StubRequeueHandler[Task, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Task.now(Requeue)

      app.publish(Payload.from(1)).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("It should requeue the message if handler returns a failed future and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Task, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Task.fail(new RuntimeException("Handler problem"))

      app.publish(Payload.from(1)).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("(Raw) requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Task, Delivery]
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      handler.nextResponse = Task.fail(new RuntimeException("Handler problem"))

      app.publish(Payload.from(1)).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("Requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[Task, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextException = Some(new RuntimeException("Handler problem"))

      app.publish(Payload.from(1)).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }


  test(s"It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {
    val handler = new StubRequeueHandler[Task, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Task.now(Requeue)

      val payload = Payload.from(1)
      app.publish(payload).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        app.dlqHandler.get.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }


  test(s"It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
    val handler = new StubRequeueHandler[Task, Int]
    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1, 1.second)
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)) { app =>
      handler.nextResponse = Task.now(Requeue)

      val payload = Payload.from(1)
      app.publish(payload).unsafePerformSyncAttempt shouldBe published

      eventually {
        handler.receivedMessages.size shouldBe 1
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        app.dlqHandler.get.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }


}