package com.itv.bucky

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import org.scalatest.{FunSuite, Matchers}
import com.itv.bucky.SuperTest.{StubChannel, withDefaultClient}
import com.itv.bucky.publish.{PendingConfirmListener, PublishCommandBuilder}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.concurrent.duration._
import cats.implicits._

class PublisherTest extends FunSuite with Matchers {
  val exchange = ExchangeName("anexchange")
  val queue    = QueueName("aqueue")
  val rk       = RoutingKey("ark")
  val message  = "Hello"
  val commandBuilder: consume.PublishCommand = PublishCommandBuilder
    .publishCommandBuilder[String](StringPayloadMarshaller)
    .using(exchange)
    .using(rk)
    .toPublishCommand(message)

  test("A message publishing should only complete when an ack is returned.") {
    val channel                               = StubChannel.publishTimeout
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    implicit val timer: Timer[IO]             = IO.timer(ec)
    implicit val cs: ContextShift[IO]         = IO.contextShift(ec)
    withDefaultClient(publishTimeout = 30.seconds, channel = channel)({ client =>
      for {
        pubSeq       <- IO(channel.publishSeq)
        future       <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _            <- IO.sleep(5.seconds)
        isCompleted1 <- IO(future.isCompleted)
        _            <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
        _            <- IO.fromFuture(IO(future)).timeout(3.seconds) //no point waiting for the initial timeout
      } yield {
        isCompleted1 shouldBe false
      }
    })
  }

  test("A message publishing should timeout if no ack is ever received.") {
    val channel                               = StubChannel.publishTimeout
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    implicit val timer: Timer[IO]             = IO.timer(ec)
    implicit val cs: ContextShift[IO]         = IO.contextShift(ec)
    withDefaultClient(publishTimeout = 1.seconds, channel = channel)({ client =>
      for {
        future      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        result      <- IO.fromFuture(IO(future)).attempt
        listeners   <- IO(channel.confirmListeners.map(_.asInstanceOf[PendingConfirmListener[IO]]))
        pendingConf <- listeners.toList.map(_.pendingConfirmations.get).sequence
      } yield {
        result shouldBe 'left
        result.left.get shouldBe a[TimeoutException]
        withClue("Confirm listeners should be popped") {
          pendingConf.flatMap(_.values) shouldBe 'empty
        }
      }
    })
  }

  test("Multiple messages can be ack when the channel acks multiple messages") {
    val channel                               = StubChannel.publishTimeout
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    implicit val timer: Timer[IO]             = IO.timer(ec)
    implicit val cs: ContextShift[IO]         = IO.contextShift(ec)
    withDefaultClient(publishTimeout = 30.seconds, channel = channel)({ client =>
      for {
        publish1      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish2      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish3      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _             <- IO.sleep(5.seconds)
        pubSeq        <- IO(channel.publishSeq)
        areCompleted1 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
        _             <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq - 1, true)))
        _             <- IO.fromFuture(IO(Future.sequence(List(publish1, publish2, publish3)))).timeout(3.seconds) //no point waiting for the initial timeout
        areCompleted2 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
      } yield {
        areCompleted1 shouldBe List(false, false, false)
        areCompleted2 shouldBe List(true, true, true)
      }
    })
  }

  test("Multiple messages can be Nack when the channel Nacks multiple messages") {
    val channel                               = StubChannel.publishTimeout
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    implicit val timer: Timer[IO]             = IO.timer(ec)
    implicit val cs: ContextShift[IO]         = IO.contextShift(ec)
    withDefaultClient(publishTimeout = 30.seconds, channel = channel)({ client =>
      for {
        publish1      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish2      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish3      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _             <- IO.sleep(5.seconds)
        pubSeq        <- IO(channel.publishSeq)
        areCompleted1 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
        _             <- IO(channel.confirmListeners.foreach(_.handleNack(pubSeq - 1, true)))
        result        <- IO.fromFuture(IO(Future.sequence(List(publish1.attempt, publish2.attempt, publish3.attempt))))
        areCompleted2 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
      } yield {
        areCompleted1 shouldBe List(false, false, false)
        areCompleted2 shouldBe List(true, true, true)
        val result1 :: result2 :: result3 :: Nil = result
        result1 shouldBe 'left
        result1.left.get shouldBe a[RuntimeException]

        result2 shouldBe 'left
        result2.left.get shouldBe a[RuntimeException]

        result3 shouldBe 'left
        result3.left.get shouldBe a[RuntimeException]
      }
    })
  }

  test("Multiple messages can be published and some can be acked and some can be Nacked.") {
    val channel                               = StubChannel.publishTimeout
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    implicit val timer: Timer[IO]             = IO.timer(ec)
    implicit val cs: ContextShift[IO]         = IO.contextShift(ec)
    withDefaultClient(publishTimeout = 10.seconds, channel = channel)({ client =>
      for {
        publish1Seq   <- IO(channel.publishSeq)
        publish1      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish2      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publishSeq3   <- IO(channel.publishSeq)
        publish3      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _             <- IO.sleep(3.seconds)
        areCompleted1 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
        _             <- IO(channel.confirmListeners.foreach(_.handleAck(publish1Seq, false))) //this should do nothing
        _             <- IO(channel.confirmListeners.foreach(_.handleNack(publishSeq3, true))) //this should do nothing
        _             <- IO.sleep(3.seconds)
        result        <- IO.fromFuture(IO(Future.sequence(List(publish1.attempt, publish2.attempt, publish3.attempt))))
        areCompleted2 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted))
      } yield {
        areCompleted1 shouldBe List(false, false, false)
        areCompleted2 shouldBe List(true, true, true)
        val result1 :: result2 :: result3 :: Nil = result
        result1 shouldBe 'right

        result2 shouldBe 'left
        result2.left.get shouldBe a[RuntimeException]

        result3 shouldBe 'left
        result3.left.get shouldBe a[RuntimeException]
      }
    })
  }

}
