import java.util.UUID
import java.util.concurrent.Executors

import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.consume._
import com.itv.bucky.decl.Exchange
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.publish._
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.{RecordingHandler, RecordingRequeueHandler}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds

class RequeueWithExpiryActionIntegrationTest extends AnyFunSuite with Eventually with IntegrationPatience {

  case class TestFixture(
                          stubHandler: RecordingRequeueHandler[IO, String],
                          dlqHandler: RecordingHandler[IO, String],
                          publishCommandBuilder: PublishCommandBuilder.Builder[String], publisher: Publisher[IO, PublishCommand])

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO]     = IO.timer(ec)
  val requeuePolicy = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  def withTestFixture[F[_]](onRequeueExpiryAction: String => IO[ConsumeAction],
                            handlerAction: String => IO[Unit] = _ => IO.unit)
                              (test: TestFixture => IO[Unit]): Unit = {

    implicit val payloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password"))

    val exchangeName = ExchangeName(UUID.randomUUID().toString)
    val routingKey = RoutingKey(UUID.randomUUID().toString)
    val queueName = QueueName(UUID.randomUUID().toString)
    val deadletterQueueName = QueueName(s"${queueName.value}.dlq")

    val declarations = List(
      Exchange(exchangeName).binding(routingKey -> queueName)
    ) ++ requeue.requeueDeclarations(queueName, routingKey)

    AmqpClient[IO](config).use { client =>
      val handler = new RecordingRequeueHandler[IO, String](Kleisli(handlerAction).andThen(_ => IO(Requeue)).run)
      val dlqHandler = StubHandlers.ackHandler[IO, String]

      Resource.liftF(client.declare(declarations)).flatMap(_ =>
        for {
          _ <-  client.registerRequeueConsumerOf[String](
            queueName = queueName,
            handler = handler,
            requeuePolicy = requeuePolicy,
            onRequeueExpiryAction = onRequeueExpiryAction
          )
          _ <- client.registerConsumerOf(deadletterQueueName, dlqHandler)
        }
          yield ()
      ).use { _ =>
        val pub = client.publisher()
        val pcb = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
        val fixture = TestFixture(handler, dlqHandler, pcb, pub)
        test(fixture)
      }
    }.unsafeRunSync()
  }


  test("Should record an invocation as true and DeadLetter after exhausting maximum requeue attempts") {

    val invocationRecorder = ListBuffer.empty[Boolean]
    val expiryAction = (_: String) => IO.delay(invocationRecorder += true).as(DeadLetter)
    val handlerAction = (_: String) => IO.delay(invocationRecorder += false) >> IO.unit

    withTestFixture(expiryAction, handlerAction) { testFixture =>
      val publishCommand =
        testFixture.publishCommandBuilder.toPublishCommand("hello, world!")

      for {
        _ <- testFixture.publisher(publishCommand)
      }
        yield eventually {
          testFixture.stubHandler.receivedMessages.size should be (requeuePolicy.maximumProcessAttempts)
          testFixture.dlqHandler.receivedMessages.size shouldBe 1
          invocationRecorder should contain theSameElementsAs List(false, false, false, false, false, true)
        }
    }
  }

}
