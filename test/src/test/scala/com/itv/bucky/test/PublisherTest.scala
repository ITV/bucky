package com.itv.bucky.test

import cats.effect.IO
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.publish._
import com.itv.bucky.{ExchangeName, QueueName, RoutingKey, consume}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Future, TimeoutException}

class PublisherTest extends FunSuite with Matchers with IOAmqpClientTest {
  val exchange = ExchangeName("anexchange")
  val queue    = QueueName("aqueue")
  val rk       = RoutingKey("ark")
  val message  = "Hello"
  val commandBuilder: PublishCommand = PublishCommandBuilder
    .publishCommandBuilder[String](StringPayloadMarshaller)
    .using(exchange)
    .using(rk)
    .toPublishCommand(message)

  test("A message publishing should only complete when an ack is returned.") {
    val channel                               = StubChannels.publishTimeout[IO]
    runAmqpTest(client(channel, Config.empty(10.seconds))) { client =>
      for {
        pubSeq       <- IO(channel.publishSeq)
        future       <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _            <- IO.sleep(3.seconds)
        isCompleted1 <- IO(future.isCompleted)
        _            <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
        _            <- IO.fromFuture(IO(future)).timeout(3.seconds) //no point waiting for the initial timeout
      } yield {
        isCompleted1 shouldBe false
      }
    }
  }

  test("A message publishing should timeout if no ack is ever received.") {
    val channel                               = StubChannels.publishTimeout[IO]
    runAmqpTest(client(channel, Config.empty(1.second))) { client =>
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
    }
  }

  test("Multiple messages can be ack when the channel acks multiple messages") {
    val channel                               = StubChannels.publishTimeout[IO]
    runAmqpTest(client(channel, Config.empty(30.seconds))) { client =>
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
    }
  }

  test("Multiple messages can be Nack when the channel Nacks multiple messages") {
    val channel                               = StubChannels.publishTimeout[IO]
    runAmqpTest(client(channel, Config.empty(15.seconds))) { client =>
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
    }
  }

  test("Multiple messages can be published and some can be acked and some can be Nacked.") {
    val channel                               = StubChannels.publishTimeout[IO]
    runAmqpTest(client(channel, Config.empty(10.seconds))) { client =>
      for {
        publish1      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish2      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish3      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        publish4      <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _             <- IO.sleep(3.seconds)
        areCompleted1 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted, publish4.isCompleted))
        _             <- IO(channel.confirmListeners.foreach(_.handleAck(0, false))) // ack 0
        _             <- IO(channel.confirmListeners.foreach(_.handleNack(2, true))) //nack 1, 2
        result <- IO.fromFuture(
          IO(Future.sequence[Either[Throwable, Unit], List](List(publish1.attempt, publish2.attempt, publish3.attempt, publish4.attempt))))
        areCompleted2 <- IO(List(publish1.isCompleted, publish2.isCompleted, publish3.isCompleted, publish4.isCompleted))
      } yield {
        println(result)
        areCompleted1 shouldBe List(false, false, false, false)
        areCompleted2 shouldBe List(true, true, true, true)

        result.filter(_.isRight) should have size 1
        result.filter(r => r.isLeft && r.left.get.isInstanceOf[RuntimeException]) should have size 2
        result.filter(r => r.isLeft && r.left.get.isInstanceOf[TimeoutException]) should have size 1
      }
    }
  }

}
