package com.itv.bucky.suite

import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky._
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually

import scala.util.{Random, Success}
import scala.concurrent.duration._
import scala.language.higherKinds
import Eventually.eventually

trait RequeueIntegrationTest[F[_]]
    extends FunSuite
    with PublisherConsumerBaseTest[F]
    with StrictLogging
    with EffectMonad[F, Throwable] {

  implicit val requeuePatienceConfig: Eventually.PatienceConfig =
    Eventually.PatienceConfig(timeout = 10.second, interval = 100.millis)

  import org.scalatest.Matchers._

  implicit val intMessageDeserializer = StringPayloadUnmarshaller.map(_.toInt)

  implicit val intMarshaller: PayloadMarshaller[Int] = StringPayloadMarshaller.contramap(_.toString)

  val exchange = ExchangeName("")

  val requeuePolicy = RequeuePolicy(3, 1.second)

  test(s"Should retain any custom headers when republishing") {
    val handler = new StubRequeueHandler[F, Delivery]()
    handler.nextResponse = effectMonad(Requeue)
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      val properties = MessageProperties.persistentTextPlain.withHeader("foo" -> "bar")

      verifySuccess(app.publish(Any.payload(), properties))

      eventually {
        val headersOfReceived = handler.receivedMessages.map(d => HeaderExt("foo", d.properties))
        headersOfReceived.flatten.toList.length should be > 1
      }
    }
  }

  test("Should retain any custom amqp properties when republishing") {
    val handler = new StubRequeueHandler[F, Delivery]
    handler.nextResponse = effectMonad(Requeue)
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      val expectedCorrelationId: Option[String] = Some("banana")
      val properties                            = MessageProperties.persistentTextPlain.copy(correlationId = expectedCorrelationId)
      verifySuccess(app.publish(Payload.from("Hello World!"), properties))

      eventually {
        handler.receivedMessages.count(_.properties.correlationId == expectedCorrelationId) should be > 1
      }
    }
  }

  test("It should not requeue when the handler Acks") {
    val handler = new StubRequeueHandler[F, Int]
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      verifySuccess(app.publish(Payload.from(1)))

      eventually {
        handler.receivedMessages.length shouldBe 1
      }
      app.dlqHandler.get.receivedMessages shouldBe 'empty
      app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
    }
  }

  test("It should reprocess the message at least maximumProcessAttempts times upon repeated requeue") {
    val handler = new StubRequeueHandler[F, Int]
    handler.nextResponse = effectMonad(Requeue)
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      verifySuccess(app.publish(Payload.from(1)))

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test("It should requeue the message if handler returns a failed future and is configured to requeue on failure") {
    val handler = new StubRequeueHandler[F, Int]
    handler.nextResponse = effectMonad.raiseError(new RuntimeException("Handler problem"))
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      verifySuccess(app.publish(Payload.from(1)))

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test(
    "(Raw) requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {

    val handler = new StubRequeueHandler[F, Delivery]
    handler.nextResponse = effectMonad.raiseError(new RuntimeException("Handler problem"))
    withPublisherAndConsumer(requeueStrategy = RawRequeue(handler, requeuePolicy)) { app =>
      verifySuccess(app.publish(Payload.from(1)))

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test(
    "Requeue consumer should requeue the message if handler throws an exception and is configured to requeue on failure") {

    val handler = new StubRequeueHandler[F, Int]
    handler.nextException = Some(new RuntimeException("Handler problem"))
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      verifySuccess(app.publish(Payload.from(1)))

      eventually {
        handler.receivedMessages.length should be >= requeuePolicy.maximumProcessAttempts
      }
    }
  }

  test(s"It should deadletter the message after maximumProcessAttempts unsuccessful attempts to process") {

    val handler = new StubRequeueHandler[F, Int]
    handler.nextResponse = effectMonad(Requeue)
    withPublisherAndConsumer(requeueStrategy = TypeRequeue(handler, requeuePolicy, intMessageDeserializer)) { app =>
      val payload = Payload.from(1)
      verifySuccess(app.publish(payload))

      eventually {
        handler.receivedMessages.size shouldBe requeuePolicy.maximumProcessAttempts
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        app.dlqHandler.get.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }

  test(s"It should deadletter the message if requeued and maximumProcessAttempts is < 1") {
    val handler = new StubRequeueHandler[F, Int]
    handler.nextResponse = effectMonad(Requeue)
    val negativeProcessAttemptsRequeuePolicy = RequeuePolicy(Random.nextInt(10) * -1, 1.second)
    withPublisherAndConsumer(
      requeueStrategy = TypeRequeue(handler, negativeProcessAttemptsRequeuePolicy, intMessageDeserializer)) { app =>
      val payload = Payload.from(1)
      verifySuccess(app.publish(payload))

      eventually {
        handler.receivedMessages.size shouldBe 1
        app.amqpClient.estimatedMessageCount(app.queueName) shouldBe Success(0)
        app.amqpClient.estimatedMessageCount(app.requeueQueueName) shouldBe Success(0)
        app.dlqHandler.get.receivedMessages.map(_.body) shouldBe List(payload)
      }
    }
  }
}
