import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.itv.bucky._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.RecordingHandler
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds

class HammerTest extends FunSuite with Eventually with IntegrationPatience {

  case class TestFixture(stubHandler: RecordingHandler[IO, String], publisher: Publisher[IO, String])

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO]     = IO.timer(ec)

  def withTestFixture(test: TestFixture => IO[Unit]): Unit = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password"))
    implicit val payloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val exchangeName = ExchangeName(UUID.randomUUID().toString)
    val routingKey = RoutingKey(UUID.randomUUID().toString)
    val queueName = QueueName(UUID.randomUUID().toString)

    val declarations = List(
      Queue(queueName).autoDelete.expires(20.minutes),
      Exchange(exchangeName).binding(routingKey -> queueName).autoDelete.expires(20.minutes)
    )

    AmqpClient[IO](config).use { client =>
      val handler = StubHandlers.ackHandler[IO, String]
      for {
        _ <- client.declare(declarations)
        _ <- client.registerConsumerOf(queueName, handler)
        pub = client.publisherOf[String](exchangeName, routingKey)
        fixture = TestFixture(handler, pub)
        _ <- test(fixture)
      } yield ()
    }.unsafeRunSync()
  }

  test("can handle concurrency") {
    withTestFixture { testFixture =>
      val hammerStrength = 10000
      val parallelPublish = 250

      val results =
        (1 to hammerStrength)
        .grouped(parallelPublish)
        .toList
        .flatTraverse(group => {
          group.toList.parTraverse { i =>
            val deferred = Deferred[IO, Option[Throwable]].unsafeRunSync()
            testFixture.publisher(s"hello$i").runAsync {
              case Right(_) => deferred.complete(None)
              case Left(error) => deferred.complete(Some(error))
            }.toIO.flatMap(_ => deferred.get)
          }.map(_.flatten)
        })

      for {
        errors <- results
      }
        yield eventually {
          errors shouldBe empty
          testFixture.stubHandler.receivedMessages should have size hammerStrength
          val set = TreeSet(testFixture.stubHandler.receivedMessages: _*)
          (1 to hammerStrength).foreach(i => set.contains(s"hello$i") shouldBe true)
        }
    }
  }

}