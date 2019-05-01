package com.itv.bucky.kamonSupport

import java.util.concurrent.Executors

import cats.effect.{IO, Resource}
import com.itv.bucky.consume.{Ack, ConsumeAction, DeadLetter}
import com.itv.bucky.{AmqpClient, ExchangeName, QueueName, RoutingKey}
import com.itv.bucky.decl.{Declaration, Exchange, Queue}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FunSuite, Matchers}
import com.itv.bucky.test._
import com.itv.bucky._
import com.itv.bucky.kamonSupport._
import com.itv.bucky.test.stubs.{RecordingHandler, StubChannel, StubPublisher}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import cats._
import cats.implicits._
import cats.effect._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.publish.PublishCommandBuilder
import kamon.executors.util.ContextAwareExecutorService
import kamon.trace.Span.TagValue
import kamon.trace.{IdentityProvider, Span}

class KamonSupportTest extends FunSuite with Matchers with Eventually with SpanSupport {
  /*val queue = Queue(QueueName("kamon-spec-test"))
  val rk    = RoutingKey("kamon-spec-rk")
  val exchange = Exchange(ExchangeName("kamon-spec-exchange"))
    .binding(rk -> queue.name)

  test("Propagate the context.") {
    withPreDeclaredConsumer() { (reporter, publisher, _) =>
      for {
        _ <- publisher("some string")
      } yield {
        eventually(reporter.spans should have size 2)
        val publishSpan = reporter.spans.find(_.operationName == s"bucky.publish.exchange.${exchange.name.value}").get
        val consumeSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        publishSpan.context.spanID shouldBe consumeSpan.context.parentID
        publishSpan.context.traceID shouldBe consumeSpan.context.traceID
      }
    }
  }

  test("Register errors.") {
    withPreDeclaredConsumer(DeadLetter) { (reporter, publisher, _) =>
      for {
        _ <- publisher("some string")
      } yield {
        eventually(reporter.spans should have size 2)
        val publishSpan  = reporter.spans.find(_.operationName == s"bucky.publish.exchange.${exchange.name.value}").get
        val consumerSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        publishSpan.tags.mapValues(fromTagValueToString) shouldBe Map(
          "span.kind" -> "bucky.publish",
          "component" -> "bucky",
          "rk"        -> rk.value,
          "exchange"  -> exchange.name.value,
          "result"    -> "success"
        )
        consumerSpan.tags.mapValues(fromTagValueToString) shouldBe Map(
          "span.kind" -> "bucky.consume",
          "component" -> "bucky",
          "rk"        -> rk.value,
          "exchange"  -> exchange.name.value,
          "result"    -> "deadletter"
        )
      }
    }
  }

  test("consumers should be able to obtain trace and span ids form incoming message hears") {
    val spanId  = new IdentityProvider.Default().spanIdGenerator().generate().string
    val traceId = new IdentityProvider.Default().traceIdGenerator().generate().string
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
        _ <- channel.publish(commandWSpan)
      } yield {
        eventually(reporter.spans should have size 1)
        val consumeSpan = reporter.spans.find(_.operationName == s"bucky.consume.${queue.name.value}").get
        consumeSpan.context.traceID.string shouldBe traceId
        consumeSpan.context.parentID.string shouldBe spanId
      }
    }
  }

  def withPreDeclaredConsumer(consumeAction: ConsumeAction = Ack)(
      test: (AccTestSpanReporter, Publisher[IO, String], RecordingHandler[IO, String]) => IO[Unit]): Unit =
    withSpanReporter { reporter =>
      {
        val handler        = StubHandlers.recordingHandler[IO, String](_ => IO.delay(consumeAction))
        val declarations   = List(queue, exchange) ++ exchange.bindings
        val executor       = ContextAwareExecutorService(Executors.newFixedThreadPool(10))
        implicit val ec    = ExecutionContext.fromExecutor(executor)
        implicit val timer = IO.timer(ec)
        implicit val cs    = IO.contextShift(ec)
        val result = IOAmqpClientTest(ec, timer, cs)
          .clientForgiving()
          .map(_.withKamonSupport())
          .use(client => {
            for {
              _      <- cs.shift
              _      <- client.declare(declarations)
              _      <- client.registerConsumerOf(queue.name, handler)
              result <- test(reporter, client.publisherOf[String](exchange.name, rk), handler).attempt
            } yield result
          })
          .unsafeRunSync()
        IO.fromEither(result).unsafeRunSync()
      }
    }

  def withChannel(test: (AccTestSpanReporter, Channel[IO]) => IO[Unit]) =
    withSpanReporter { reporter =>
      {
        val handler        = StubHandlers.recordingHandler[IO, String](_ => IO.delay(Ack))
        val declarations   = List(queue, exchange) ++ exchange.bindings
        val executor       = ContextAwareExecutorService(Executors.newFixedThreadPool(10))
        implicit val ec    = ExecutionContext.fromExecutor(executor)
        implicit val timer = IO.timer(ec)
        implicit val cs    = IO.contextShift(ec)
        val actualChannel  = StubChannels.forgiving[IO]
        val channel        = Resource.make(IO(actualChannel))(_.close())
        val client         = AmqpClient.apply[IO](Config.empty(3.seconds), channel.map(_.asInstanceOf[Channel[IO]]))

        val result = client
          .use(client => {
            for {
              _      <- cs.shift
              _      <- client.declare(declarations)
              _      <- client.withKamonSupport().registerConsumerOf(queue.name, handler)
              result <- test(reporter, actualChannel).attempt
            } yield result
          })
          .unsafeRunSync()
        IO.fromEither(result).unsafeRunSync()
      }
    }

  def fromTagValueToString(tagValue: TagValue): String =
    tagValue match {
      case boolean: TagValue.Boolean => s"${boolean.text}"
      case TagValue.String(string)   => s"$string"
      case TagValue.Number(number)   => s"$number"
    }*/
}
