package com.itv.bucky.test

import cats.effect.kernel.Outcome.Errored
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Outcome}
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.publish._
import com.itv.bucky.{ExchangeName, QueueName, RoutingKey}
import com.rabbitmq.client.AMQP.BasicProperties
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.reflect.ClassTag

class PublisherTest extends AnyFunSuite with IOAmqpClientTest with EitherValues {

  implicit val ioRuntime: IORuntime = cats.effect.unsafe.implicits.global

  def hasOutcomeError[T <: Throwable](o : Outcome[IO, Throwable, Unit])(implicit tag: ClassTag[T]) = o match {
    case Errored(e) => tag.unapply(e).isDefined
    case _ => false
  }

  val exchange = ExchangeName("anexchange")
  val queue = QueueName("aqueue")
  val rk = RoutingKey("ark")
  val message = "Hello"
  val commandBuilder: PublishCommand = PublishCommandBuilder
    .publishCommandBuilder[String](StringPayloadMarshaller)
    .using(exchange)
    .using(rk)
    .toPublishCommand(message)

  test("A message publishing should only complete when an ack is returned.") {
    val channel = StubChannels.publishNoAck[IO]
    runAmqpTestIO(client(channel, Config.empty(10.seconds))) { client =>
      for {
        pubSeq <- IO(channel.publishSeq)
        future <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        _ <- IO.sleep(3.seconds)
        _ <- IO.sleep(3.seconds)
        isCompleted1 <- IO(future.isCompleted)
        _ <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
        _ <- IO.fromFuture(IO(future)).timeout(3.seconds) //no point waiting for the initial timeout
      } yield isCompleted1 shouldBe false
    }
  }

  test("A message publishing should error when a return is returned.") {
    val channel = StubChannels.publishNoAck[IO]
    runAmqpTest(client(channel, Config.empty(10.seconds))) { client =>
      for {
        pubSeq <- IO(channel.publishSeq)
        future <- IO(client.publisher()(commandBuilder).unsafeToFuture())
        properties = new BasicProperties
        _          = properties.builder().build()
        _      <- IO(channel.returnListeners.foreach(_.handleReturn(400, "reply", exchange.value, rk.value, properties, message.getBytes)))
        _      <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
        result <- IO.fromFuture(IO(future)).attempt
      } yield {
        result shouldBe 'left
      }
    }
  }

  test("A message publishing should timeout if no ack is ever received.") {
    val channel = StubChannels.publishNoAck[IO]
    runAmqpTestIO(client(channel, Config.empty(1.second))) { client =>
      for {
        result <- client.publisher()(commandBuilder).attempt
        listeners <- IO(channel.confirmListeners.map(_.asInstanceOf[PendingConfirmListener[IO]]))
        pendingConf <- listeners.toList.map(_.pendingConfirmations.get).sequence
      } yield {
        result.left.value shouldBe a[TimeoutException]
        withClue("Confirm listeners should be popped") {
          pendingConf.flatMap(_.values) shouldBe empty
        }
      }
    }
  }


  test("Multiple messages can be ack when the channel acks multiple messages") {
    val channel = StubChannels.publishNoAck[IO]
    runAmqpTestIO(client(channel, Config.empty(30.seconds))) { client =>
      for {
        fiber1 <- client.publisher()(commandBuilder).start
        fiber2 <- client.publisher()(commandBuilder).start
        fiber3 <- client.publisher()(commandBuilder).start
        _ <- IO.sleep(5.seconds)
        _ <- IO(channel.confirmListeners.foreach(_.handleAck(2, true)))
        outcome1 <- fiber1.join
        outcome2 <- fiber2.join
        outcome3 <- fiber3.join
      } yield {
        assert(outcome1.isSuccess)
        assert(outcome2.isSuccess)
        assert(outcome3.isSuccess)
      }
    }
  }

  test("Multiple messages can be Nack when the channel Nacks multiple messages") {
    val channel = StubChannels.publishNoAck[IO]
    runAmqpTestIO(client(channel, Config.empty(30.seconds))) { client =>
      for {
        fiber1 <- client.publisher()(commandBuilder).start
        fiber2 <- client.publisher()(commandBuilder).start
        fiber3 <- client.publisher()(commandBuilder).start
        _ <- IO.sleep(5.seconds)
        _ <- IO(channel.confirmListeners.foreach(_.handleNack(2, true)))
        outcome1 <- fiber1.join
        outcome2 <- fiber2.join
        outcome3 <- fiber3.join
      } yield {
        assert(hasOutcomeError[RuntimeException](outcome1))
        assert(hasOutcomeError[RuntimeException](outcome2))
        assert(hasOutcomeError[RuntimeException](outcome3))
      }
    }
  }

  test("Multiple messages can be published and some can be acked and some can be Nacked.") {
    val channel = StubChannels.publishNoAck[IO]
      runAmqpTestIO(client(channel, Config.empty(10.seconds))) { client =>
        val pub: IO[Unit] = client.publisher()(commandBuilder)
        for {
          fiber1 <- client.publisher()(commandBuilder).start
          fiber2 <- client.publisher()(commandBuilder).start
          fiber3 <- client.publisher()(commandBuilder).start
          fiber4 <- client.publisher()(commandBuilder).start
          _             <- IO.sleep(3.seconds)
          _             <- IO.sleep(3.seconds)
          _ <- IO(channel.confirmListeners.foreach(_.handleNack(0, false)))
          _ <- IO(channel.confirmListeners.foreach(_.handleNack(1, false))) //nack 1, 2
          _ <- IO(channel.confirmListeners.foreach(_.handleAck(2, false))) // ack 0
          outcome1 <- fiber1.join
          outcome2 <- fiber2.join
          outcome3 <- fiber3.join
          outcome4 <- fiber4.join
        } yield {
          val outcomes = List(outcome1, outcome2, outcome3, outcome4)
          println(outcomes.toString())
          outcomes.count(_.isSuccess) should ===(1)
          outcomes.count(hasOutcomeError[RuntimeException](_)) should ===(2)
          outcomes.count(hasOutcomeError[TimeoutException](_)) should ===(1)
        }
      }
    }
}
