package com.itv.bucky.integrationTest

import cats.effect.testing.scalatest.EffectTestSupport
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.consume.Ack
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.test.StubHandlers
import com.itv.bucky.test.stubs.RecordingHandler
import com.itv.bucky.test.stubs.RecordingHandler.ConsumeActionBufferRef
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.language.higherKinds

class HammerTest extends AsyncFunSuite with EffectTestSupport with Eventually with IntegrationPatience with StrictLogging with Matchers {

  implicit override val ioRuntime: IORuntime = packageIORuntime
  case class TestFixture(stubHandler: RecordingHandler[IO, String], publisher: Publisher[IO, String], client: AmqpClient[IO])

  def withTestFixture(test: TestFixture => IO[Unit]): IO[Unit] = {
    val rawConfig = ConfigFactory.load
    val config =
      AmqpClientConfig(
        rawConfig.getString("rmq.host"),
        rawConfig.getInt("rmq.port"),
        rawConfig.getString("rmq.username"),
        rawConfig.getString("rmq.password")
      )
    implicit val payloadMarshaller: PayloadMarshaller[String]     = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val exchangeName = ExchangeName(UUID.randomUUID().toString)
    val routingKey   = RoutingKey(UUID.randomUUID().toString)
    val queueName    = QueueName(UUID.randomUUID().toString)

    val declarations = List(
      Queue(queueName).autoDelete.expires(20.minutes),
      Exchange(exchangeName).binding(routingKey -> queueName).autoDelete.expires(20.minutes)
    )

    val testInfra = for {
      client <- AmqpClient[IO](config)
      ref <- RecordingHandler.createConsumeActionBufferRef[IO, String]()
    } yield (client, ref)

    testInfra.use { case (client, ref) =>
      val handler = StubHandlers.ackHandler[IO, String](ref)
      val handlerResource =
        Resource.eval(client.declare(declarations)).flatMap(_ => client.registerConsumerOf(queueName, handler))

      handlerResource.use { _ =>
        val pub     = client.publisherOf[String](exchangeName, routingKey)
        val fixture = TestFixture(handler, pub, client)
        test(fixture)
      }
    }
  }

  test("can handle concurrency") {
    withTestFixture { testFixture =>
      val hammerStrength  = 10000
      val parallelPublish = 250

//      val results =
//        (1 to hammerStrength)
//          .grouped(parallelPublish)
//          .toList
//          .flatTraverse { group =>
//            group
//              .toList
//              .parTraverse { i =>
//                testFixture.publisher(s"hello$i").start
//              }
//          }

      val results = (1 to hammerStrength).toList.parTraverse { i =>
        testFixture.publisher(s"hello$i").start
      }

      for {
        listOfOutcomes <- results.flatMap(_.parTraverse(_.join))
        receivedMessages <- testFixture.stubHandler.receivedMessages
//        errors <- results
      } yield eventually {
//        errors shouldBe empty
        println(s"errors = ${listOfOutcomes.count(_.isError)}")
        println(s"success = ${listOfOutcomes.count(_.isSuccess)}")
        println(s"cancelled = ${listOfOutcomes.count(_.isCanceled)}")
        listOfOutcomes.forall(_.isSuccess) shouldBe true

//        errors shouldBe empty
        receivedMessages should have size hammerStrength
        val set = TreeSet(receivedMessages: _*)
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

      val fastHandler = { ref: ConsumeActionBufferRef[IO, String] =>
        new RecordingHandler[IO, String]((v1: String) =>
          for {
            _ <- order.update(_ :+ "fast")
          } yield Ack
        , ref)
      }

      val slowHandler = { ref: ConsumeActionBufferRef[IO, String] =>
        new RecordingHandler[IO, String]((v1: String) =>
          for {
            _ <- IO.sleep(10.second)
            _ <- order.update(_ :+ "slow")
          } yield Ack
        ,ref)
      }

      val declarations =
        List(Queue(slowQueue), Queue(fastQueue), Exchange(exchange).binding(slowRk -> slowQueue).binding(fastRk -> fastQueue))

      val handlers =
        for {
          slowHandlerBufferRef <- RecordingHandler.createConsumeActionBufferRef[IO, String]
          fastHandlerBufferRef <- RecordingHandler.createConsumeActionBufferRef[IO, String]
          _ <- testFixture.client.registerConsumerOf(slowQueue, slowHandler(slowHandlerBufferRef))
          _ <- testFixture.client.registerConsumerOf(fastQueue, fastHandler(fastHandlerBufferRef))
        } yield ()

      val handlersResource =
        Resource.eval(testFixture.client.declare(declarations)).flatMap(_ => handlers)

      handlersResource.use { _ =>
        for {
          _ <- slowPublisher("slow one")
          _ <- IO.sleep(1.second)
          _ <- fastPublisher("fast one")
        } yield eventually {
          val messages = order.get.unsafeRunSync()
          messages should have size 2
          messages shouldBe List("fast", "slow")
        }
      }
    }
  }

}
