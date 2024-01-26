package com.itv.bucky

import cats.effect.testing.scalatest.EffectTestSupport
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
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

import java.time.{Clock, Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ShutdownTimeoutTest extends AsyncFunSuite with IntegrationSpec with EffectTestSupport with Eventually with IntegrationPatience {

  case class TestFixture(
                          stubHandler: RecordingRequeueHandler[IO, Delivery],
                          dlqHandler: RecordingHandler[IO, Delivery],
                          publishCommandBuilder: PublishCommandBuilder.Builder[String],
                          publisher: Publisher[IO, PublishCommand]
                        )

  implicit override val ioRuntime: IORuntime = packageIORuntime
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  val requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  def runTest[A](test: IO[A]): IO[A] = {
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
    val exchangeName                                              = ExchangeName(UUID.randomUUID().toString)
    val routingKey                                                = RoutingKey(UUID.randomUUID().toString)
    val queueName                                                 = QueueName(UUID.randomUUID().toString)
    val declarations = List(Exchange(exchangeName).binding(routingKey -> queueName)) ++ requeue.requeueDeclarations(queueName, routingKey)

    JavaBackendAmqpClient[IO](config)
      .use { client =>
        val handler = StubHandlers.recordingHandler[IO, Delivery]((_: Delivery) => IO.sleep(3.seconds).map(_ => Ack))
        Resource
          .eval(client.declare(declarations))
          .flatMap(_ =>
            for {
              _ <- client.registerConsumer(queueName, handler)
            } yield ()
          )
          .use { _ =>
            val pcb = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
            client.publisher()(pcb.toPublishCommand("a message")).flatMap(_ => test)
          }
      }
  }

  test("Should wait until a handler finishes executing before shuttind down") {
    val clock = Clock.systemUTC()
    val start = Instant.now(clock)
    runTest[Instant](IO.delay(Instant.now())).map { result =>
      val after = Instant.now(clock)
      println(LocalDateTime.ofInstant(start, ZoneOffset.UTC))
      println(LocalDateTime.ofInstant(after, ZoneOffset.UTC))
      (after.toEpochMilli - start.toEpochMilli) > 3000 shouldBe true
      (result.toEpochMilli - after.toEpochMilli) < 3000 shouldBe true
    }
  }
}