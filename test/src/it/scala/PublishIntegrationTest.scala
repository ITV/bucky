import cats.effect.{ContextShift, IO, Resource, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.consume.Delivery
import com.itv.bucky.decl.Exchange
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.publish.{PublishCommand, PublishCommandBuilder}
import com.itv.bucky.{
  AmqpClient,
  AmqpClientConfig,
  ExchangeName,
  PayloadMarshaller,
  PayloadUnmarshaller,
  Publisher,
  QueueName,
  RoutingKey,
  publishCommandBuilder
}
import com.itv.bucky.test.StubHandlers
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import org.scalatest.matchers.should.Matchers._

class PublishIntegrationTest extends AnyFunSuite with Eventually with IntegrationPatience {

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO]     = IO.timer(ec)
  val requeuePolicy                 = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  test("publisher should error if mandatory is true and there is no routing") {
    withTestFixture{
      case (builder, publisher) =>
        publisher(builder.usingMandatory(true).toPublishCommand("Where am I going?")).attempt.map(_ shouldBe 'left)
    }
  }
  test("publisher should publish if mandatory is false and there is no routing") {
    withTestFixture {
      case (builder, publisher) =>
        publisher(builder.usingMandatory(false).toPublishCommand("But seriously though, where am I going?")).attempt.map(_ shouldBe 'right)
    }

  }

  def withTestFixture(test: (PublishCommandBuilder.Builder[String], Publisher[IO, PublishCommand]) => IO[Unit]): Unit = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(rawConfig.getString("rmq.host"),
                       rawConfig.getInt("rmq.port"),
                       rawConfig.getString("rmq.username"),
                       rawConfig.getString("rmq.password"))
    implicit val payloadMarshaller: PayloadMarshaller[String]     = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val exchangeName = ExchangeName(UUID.randomUUID().toString)
    val routingKey   = RoutingKey(UUID.randomUUID().toString)

    AmqpClient[IO](config)
      .use { client =>
        Resource
          .liftF(
            client.declare(Exchange(exchangeName))
          )
          .use { _ =>
            val pcb = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
            val pub = client.publisher()
            test(pcb, pub)
          }
      }
      .unsafeRunSync()
  }

}
