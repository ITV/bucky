import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO, Resource, Sync, Timer}
import com.itv.bucky._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.{RecordingHandler, RecordingRequeueHandler}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import org.scalatest.Matchers._

import scala.language.higherKinds

class PublishIntegrationTest extends FunSuite with Eventually with IntegrationPatience {

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO]     = IO.timer(ec)

  def withTestFixture(test: Publisher[IO, PublishCommand] => IO[Unit]): Unit = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password"))
    implicit val payloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    AmqpClient[IO](config).use { client =>
      val pub = client.publisher()
      test(pub)
    }.unsafeRunSync()
  }

  test("Should recover from a publish to a non existent exchange") {
    withTestFixture { publisher =>
      val publishCommand =
        PublishCommandBuilder.publishCommandBuilder[String](StringPayloadMarshaller)
          .using(ExchangeName(UUID.randomUUID().toString))
          .using(RoutingKey("-"))
          .toPublishCommand("foo")

      for {
        result <- publisher(publishCommand)
      }
        yield 1 shouldBe 1
    }
  }


}