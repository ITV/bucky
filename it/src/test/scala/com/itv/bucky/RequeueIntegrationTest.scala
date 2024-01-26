package com.itv.bucky

import cats.effect.testing.scalatest.EffectTestSupport
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.backend.fs2rabbit.Fs2RabbitAmqpClient
import com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient
import com.itv.bucky.consume._
import com.itv.bucky.decl.Exchange
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.publish._
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.{RecordingHandler, RecordingRequeueHandler}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RequeueIntegrationTest extends AsyncFunSuite with IntegrationSpec with EffectTestSupport with Eventually with IntegrationPatience {

  case class TestFixture(
      stubHandler: RecordingRequeueHandler[IO, Delivery],
      dlqHandler: RecordingHandler[IO, Delivery],
      publishCommandBuilder: PublishCommandBuilder.Builder[String],
      publisher: Publisher[IO, PublishCommand]
  )

  implicit override val ioRuntime: IORuntime = packageIORuntime
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  val requeuePolicy                 = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  def withTestFixture(test: TestFixture => IO[Unit]): IO[Unit] = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password")
      )
    implicit val payloadMarshaller: PayloadMarshaller[String]     = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val exchangeName        = ExchangeName(UUID.randomUUID().toString)
    val routingKey          = RoutingKey(UUID.randomUUID().toString)
    val queueName           = QueueName(UUID.randomUUID().toString)
    val deadletterQueueName = QueueName(s"${queueName.value}.dlq")

    val declarations = List(
      Exchange(exchangeName).binding(routingKey -> queueName)
    ) ++ requeue.requeueDeclarations(queueName, routingKey)

    Fs2RabbitAmqpClient[IO](config).use { client =>
      val handler    = StubHandlers.requeueRequeueHandler[IO, Delivery]
      val dlqHandler = StubHandlers.ackHandler[IO, Delivery]

      Resource
        .eval(client.declare(declarations))
        .flatMap(_ =>
          for {
            _ <- client.registerRequeueConsumer(queueName, handler, requeuePolicy)
            _ <- client.registerConsumer(deadletterQueueName, dlqHandler)
          } yield ()
        )
        .use { _ =>
          val pub     = client.publisher()
          val pcb     = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
          val fixture = TestFixture(handler, dlqHandler, pcb, pub)
          test(fixture)
        }
    }
  }

  test("Should retain payload, custom headers and properties when republishing") {
    withTestFixture { testFixture =>
      val expectedCorrelationId: Option[String] = Some("banana")
      val properties =
        MessageProperties.persistentTextPlain
          .copy(correlationId = expectedCorrelationId)
          .withHeader("foo" -> "bar")

      val message = "hello, world!"
      val publishCommand =
        testFixture.publishCommandBuilder.using(properties).toPublishCommand(message)

      for {
        _ <- testFixture.publisher(publishCommand)
      } yield eventually {
        testFixture.stubHandler.receivedMessages.size should be > 1
        testFixture.stubHandler.receivedMessages.map(_.properties).foreach { properties =>
          properties.headers("foo").toString shouldBe "bar"
          properties.correlationId shouldBe expectedCorrelationId
        }
        testFixture.stubHandler.receivedMessages.map(_.body.unmarshal(StringPayloadUnmarshaller)).foreach {
          case Right(value) => value shouldBe message
          case _            => fail("could not unmarsal")
        }
      }
    }
  }

  test("Should deadletter after maximum process attempts exceeded") {
    withTestFixture { testFixture =>
      val publishCommand =
        testFixture.publishCommandBuilder.toPublishCommand("hello, world!")

      for {
        _ <- testFixture.publisher(publishCommand)
      } yield eventually {
        testFixture.stubHandler.receivedMessages.size should be(requeuePolicy.maximumProcessAttempts)
        testFixture.dlqHandler.receivedMessages.size shouldBe 1
      }
    }
  }

}
