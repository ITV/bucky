import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.itv.bucky._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.consume.Ack
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.RecordingHandler
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.immutable.TreeSet
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds

class HammerTest extends FunSuite with Eventually with IntegrationPatience with StrictLogging {

  case class TestFixture(stubHandler: RecordingHandler[IO, String], publisher: Publisher[IO, String], client: AmqpClient[IO])

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
        fixture = TestFixture(handler, pub, client)
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

  test("handlers should process messages in parallel") {
    import scala.concurrent.duration._

    withTestFixture { testFixture =>
      implicit val stringPayloadMarshaller: PayloadMarshaller[String] = StringPayloadMarshaller

      val exchange = ExchangeName("anexchange")
      val slowQueue = QueueName("aqueue1" + UUID.randomUUID().toString)
      val slowRk = RoutingKey("ark1")
      val fastQueue = QueueName("aqueue2" + UUID.randomUUID().toString)
      val fastRk = RoutingKey("ark2")

      val order: Ref[IO, List[String]] = Ref.of[IO, List[String]](List.empty).unsafeRunSync()

      val fastPublisher = testFixture.client.publisherOf[String](exchange, fastRk)
      val slowPublisher = testFixture.client.publisherOf[String](exchange, slowRk)

      val fastHandler = new RecordingHandler[IO, String]((v1: String) => {
        for {
          _ <- IO.delay(logger.error("banana fast invoked"))
          _ <- order.update { current =>
            logger.error("banana fast updating, current: " + current)
            current :+ "fast"
          }
        }
          yield {
            logger.error("fast yielding")
            Ack
          }
      })
      val slowHandler = new RecordingHandler[IO, String]((v1: String) => {
        for {
          _ <- IO.delay(logger.error("banana slow invoked"))
          _ <- IO.sleep(10.second)
          _ <- IO.delay(logger.error("banana slow done sleeping"))
          _ <- order.update { current =>
            logger.error("banana slow updating, current: " + current)
            current :+ "slow"
          }
        }
          yield {
            logger.error("slow yielding")
            Ack
          }
      })

      val declarations =
        List(Queue(slowQueue), Queue(fastQueue), Exchange(exchange).binding(slowRk -> slowQueue).binding(fastRk -> fastQueue))

      for {
        _ <- testFixture.client.declare(declarations)
        _ <- testFixture.client.registerConsumerOf(slowQueue, slowHandler)
        _ <- testFixture.client.registerConsumerOf(fastQueue, fastHandler)
        _ <- IO.delay(logger.error("publishing slow"))
        _ <- slowPublisher("slow one")
        _ <- IO.delay(logger.error("publishing sleep"))
        _ <- IO.sleep(1.second)
        _ <- IO.delay(logger.error("publishing fast"))
        _ <- fastPublisher("fast one")
      }
        yield {
          eventually {
            val messages = order.get.unsafeRunSync()
            messages should have size 2
            messages shouldBe List.empty
          }
        }
    }
  }


}


