package com.itv.bucky.kamonSupport

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.consume.{Ack, ConsumeAction, DeadLetter}
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.publish.PublishCommandBuilder
import com.itv.bucky.test._
import com.itv.bucky._
import kamon.instrumentation.executor.ExecutorInstrumentation
import kamon.tag.{Tag, TagSet}
import kamon.testkit.TestSpanReporter
import kamon.testkit.TestSpanReporter.BufferingSpanReporter
import kamon.trace.Identifier
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random
import cats.effect.Spawn

class KamonSupportTest extends AnyFunSuite with Matchers with Eventually with TestSpanReporter with BeforeAndAfterAll with BeforeAndAfterEach {
  val queue = Queue(QueueName("kamon-spec-test"))
  val rk    = RoutingKey("kamon-spec-rk")
  val exchange = Exchange(ExchangeName("kamon-spec-exchange"))
    .binding(rk -> queue.name)

  val reporter = testSpanReporter()

  override def afterAll(): Unit = shutdownTestSpanReporter()

  override def beforeEach(): Unit = reporter.clear()

  test("Propagate the context via the headers") {
    withPreDeclaredConsumer() { (reporter, publisher) =>
      for {
        _ <- publisher("some string")
      } yield {
        eventually(reporter.spans should have size 2)
        val publishSpan = reporter.spans.find(_.operationName == s"bucky.publish.exchange.${exchange.name.value}").get
        val consumeSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        publishSpan.id shouldBe consumeSpan.parentId
        publishSpan.trace.id shouldBe consumeSpan.trace.id
      }
    }
  }

  test("Register errors.") {
    withPreDeclaredConsumer(DeadLetter) { (reporter, publisher) =>
      for {
        _ <- publisher("some string")
      } yield {
        eventually(reporter.spans should have size 2)
        val publishSpan  = reporter.spans.find(_.operationName == s"bucky.publish.exchange.${exchange.name.value}").get
        val consumerSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        tagSetToMap(publishSpan.metricTags) shouldBe Map(
          "span.kind" -> "bucky.publish",
          "component" -> "bucky",
          "rk"        -> rk.value,
          "exchange"  -> exchange.name.value,
          "operation" -> "bucky.publish.exchange.kamon-spec-exchange",
          "error" -> "false"
        )

        tagSetToMap(publishSpan.tags) shouldBe Map(
          "result"    -> "success"
        )

        tagSetToMap(consumerSpan.metricTags) shouldBe Map(
          "span.kind" -> "bucky.consume",
          "component" -> "bucky",
          "rk"        -> rk.value,
          "exchange"  -> exchange.name.value,
          "operation" -> "bucky.consume.kamon-spec-test",
          "error" -> "false"
        )

        tagSetToMap(consumerSpan.tags) shouldBe Map(
          "result"    -> "deadletter"
        )
      }
    }
  }

  test("consumers should be able to obtain trace and span ids form incoming message headers") {
    val spanId  = Identifier.Scheme.Single.spanIdFactory.generate().string
    val traceId = Identifier.Scheme.Single.traceIdFactory.generate().string
    val headers = Map[String, AnyRef](
      "X-B3-TraceId" -> traceId,
      "X-B3-SpanId"  -> spanId,
      "X-B3-Sampled" -> "1"
    )

    val command = PublishCommandBuilder
      .publishCommandBuilder[String](StringPayloadMarshaller)
      .using(rk)
      .using(exchange.name)
      .toPublishCommand("oh boy! What a message!")
    val commandWSpan = command.copy(basicProperties = command.basicProperties.copy(headers = command.basicProperties.headers ++ headers))
    withChannel { (reporter, channel) =>
      for {
        _ <- channel.publish(1L, commandWSpan)
      } yield {
        eventually(reporter.spans should have size 1)
        val consumeSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        consumeSpan.trace.id.string shouldBe traceId
        consumeSpan.parentId.string shouldBe spanId
      }
    }
  }

  def instrument(executor: ExecutorService): ExecutorService = {
    ExecutorInstrumentation.instrument(executor, Random.nextString(10), ExecutorInstrumentation.DefaultSettings.propagateContextOnSubmit())
  }

  def withPreDeclaredConsumer(consumeAction: ConsumeAction = Ack)(
      test: (BufferingSpanReporter, Publisher[IO, String]) => IO[Unit]): Unit = {
      val handler = StubHandlers.recordingHandler[IO, String](_ => IO.delay(consumeAction))
      val declarations = List(queue, exchange) ++ exchange.bindings
      val executor = instrument(Executors.newFixedThreadPool(10))
      implicit val ec = ExecutionContext.fromExecutor(executor)
      implicit val timer = IO.timer(ec)
      implicit val cs = IO.contextShift(ec)
      val result = IOAmqpClientTest(ec, timer, cs)
        .clientForgiving()
        .map(_.withKamonSupport(true))
        .use(client => {
          (for {
            _ <- Resource.eval(client.declare(declarations))
            _ <- client.registerConsumerOf(queue.name, handler)
          } yield ()).use { _ =>
            for {
              _ <- Spawn[IO].cede
              result <- test(reporter, client.publisherOf[String](exchange.name, rk)).attempt
            } yield result
          }
        })
        .unsafeRunSync()
      IO.fromEither(result).unsafeRunSync()
    }

  def withChannel(test: (BufferingSpanReporter, Channel[IO]) => IO[Unit]) =
      {
        val handler        = StubHandlers.recordingHandler[IO, String](_ => IO.delay(Ack))
        val declarations   = List(queue, exchange) ++ exchange.bindings
        val executor       = instrument(Executors.newFixedThreadPool(10))
        implicit val ec    = ExecutionContext.fromExecutor(executor)
        implicit val timer = IO.timer(ec)
        implicit val cs    = IO.contextShift(ec)
        val actualChannel  = StubChannels.forgiving[IO]
        val channel        = Resource.make(IO(actualChannel))(_.close())
        val clientResource =
          AmqpClient.apply[IO](Config.empty(3.seconds), () => channel.map(_.asInstanceOf[Channel[IO]]), channel.map(_.asInstanceOf[Channel[IO]]))

        val result =
          (for {
            client <- clientResource
            _      <- Resource.eval(client.declare(declarations))
            _      <- client.withKamonSupport(logging = false).registerConsumerOf(queue.name, handler)
          } yield ())
            .use { _ =>
              test(reporter, actualChannel).attempt
            }
            .unsafeRunSync()

        IO.fromEither(result).unsafeRunSync()
      }


  def tagSetToMap(tagSet: TagSet): Map[String, String] =
    tagSet.all().map(t => (t.key, Tag.unwrapValue(t).toString)).toMap
}
