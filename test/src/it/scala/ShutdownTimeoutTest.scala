import java.time.{Clock, Instant, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO, Resource, Timer}
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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

class ShutdownTimeoutTest extends AnyFunSuite with Eventually with IntegrationPatience {

  case class TestFixture(stubHandler: RecordingRequeueHandler[IO, Delivery],
                         dlqHandler: RecordingHandler[IO, Delivery],
                         publishCommandBuilder: PublishCommandBuilder.Builder[String],
                         publisher: Publisher[IO, PublishCommand])

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val timer: Timer[IO]     = IO.timer(ec)
  val requeuePolicy                 = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  def runTest[A](test: IO[A]): A = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(rawConfig.getString("rmq.host"),
                       rawConfig.getInt("rmq.port"),
                       rawConfig.getString("rmq.username"),
                       rawConfig.getString("rmq.password"))
    implicit val payloadMarshaller: PayloadMarshaller[String]     = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller
    val exchangeName                                              = ExchangeName(UUID.randomUUID().toString)
    val routingKey                                                = RoutingKey(UUID.randomUUID().toString)
    val queueName                                                 = QueueName(UUID.randomUUID().toString)
    val declarations                                              = List(Exchange(exchangeName).binding(routingKey -> queueName)) ++ requeue.requeueDeclarations(queueName, routingKey)

    AmqpClient[IO](config)
      .use { client =>
        val handler = StubHandlers.recordingHandler[IO, Delivery]((_: Delivery) => IO.sleep(3.seconds).map(_ => Ack))
        Resource
          .liftF(client.declare(declarations))
          .flatMap(_ =>
            for {
              _ <- client.registerConsumer(queueName, handler)
            } yield ())
          .use { _ =>
            val pcb = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
            client.publisher()(pcb.toPublishCommand("a message")).flatMap(_ => test)
          }
      }
      .unsafeRunSync()
  }

  test("Should wait until a handler finishes executing before shuttind down") {
    val clock  = Clock.systemUTC()
    val start  = Instant.now(clock)
    val result = runTest[Instant](IO.delay(Instant.now()))
    val after  = Instant.now(clock)
    println(LocalDateTime.ofInstant(start, ZoneOffset.UTC))
    println(LocalDateTime.ofInstant(after, ZoneOffset.UTC))
    (after.toEpochMilli - start.toEpochMilli) > 3000 shouldBe true
    (result.toEpochMilli - after.toEpochMilli) < 3000 shouldBe true
  }

}
