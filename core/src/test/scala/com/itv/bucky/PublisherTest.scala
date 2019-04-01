package com.itv.bucky

import java.util
import java.util.concurrent.TimeoutException

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl.{Exchange, Queue}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.scalatest.Matchers._

import scala.collection.immutable.TreeMap
import com.itv.bucky.SuperTest.withDefaultClient

class PublisherTest extends FunSuite with ScalaFutures {

  def withConnectionManager(test: AmqpClientConnectionManager[IO] => IO[Unit]): Unit = {
    /*    val ec                            = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    AmqpClientConnectionManager[IO](AmqpClientConfig("127.0.0.1", 5672, "guest", "guest"))
      .bracket(test)(_.shutdown()).unsafeRunSync()*/
  }

  test("A message can be published") {
    withDefaultClient { client =>
      val exchange = ExchangeName("test-exchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val declarations = List(
        Queue(queue),
        Exchange(exchange).binding(rk -> queue)
      )
      client.declare(declarations).flatMap { _ =>
        client.publisher()(commandBuilder)
      }
    }
  }

  test("A message should fail to be published on a non existent exchange") {
    withDefaultClient { client =>
      val exchange = ExchangeName("non-existent-exchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      client
        .publisher()(commandBuilder)
        .attempt
        .map({
          _ shouldBe 'left
        })
    }
  }
  /*
  test("Should clear confirmations map after a timeout") {
    withConnectionManager { connectionManager =>
      val exchange = ExchangeName("test-exchange")
      val queue    = QueueName("aqueue")
      val rk       = RoutingKey(queue.value)
      val message  = "Hello"
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)
        .toPublishCommand(message)
      val ec                            = ExecutionContext.global
      implicit val cs: ContextShift[IO] = IO.contextShift(ec)
      implicit val timer: Timer[IO]     = IO.timer(ec)

      def nextDeliveryTag = IO.pure(1L)

      def publish(publishCommand: PublishCommand, signal: Deferred[IO, Boolean], deliveryTag: Long): IO[Unit] =
        IO.pure(())

      for {
        _ <- cs.shift
        map <- Ref.of[IO, TreeMap[Long, Deferred[IO, Boolean]]](TreeMap.empty)
        pendingConfirmationListener = PendingConfirmListener(map)
        before <- map.get
        failure <- connectionManager.internalPublish(1.second, commandBuilder, pendingConfirmationListener, nextDeliveryTag _, publish).attempt
        after <- pendingConfirmationListener.pendingConfirmations.get
      }
        yield {
          failure.left.get shouldBe a[TimeoutException]
          before shouldBe 'empty
          after shouldBe 'empty
        }
    }
  }*/

  test("should have a publisherOf method") {
    fail("do some work")
  }
}
