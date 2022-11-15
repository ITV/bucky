package com.itv.bucky.test

import cats.effect.kernel.Outcome.Errored
import cats.effect.std.{CountDownLatch, Dispatcher}
import cats.effect.unsafe.IORuntime
import cats.effect.{Async, IO, Outcome, Ref, Resource}
import cats.implicits._
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.publish._
import com.itv.bucky.test.stubs.StubChannel
import com.itv.bucky.{AmqpClient, AmqpClientConfig, Channel, ExchangeName, QueueName, RoutingKey}
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.Logger
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.TimeoutException
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

  case class TestInfra(amqpClient: AmqpClient[IO], stubChannel: StubChannel[IO], publishSeq: Ref[IO, Long])

  def runAmqpTestInfra(testInfraRes: Resource[IO, TestInfra])(test: TestInfra => IO[Unit])(implicit async: Async[IO]): Unit =
    testInfraRes.use(test).unsafeRunSync()

  private def infraInit(config: AmqpClientConfig, publishSleepDuration: FiniteDuration = 0.seconds)(implicit async: Async[IO]): Resource[IO, TestInfra] = {
      for {
        publishSeq <- Resource.eval(Ref[IO].of(0L))
        channel <- Resource.eval(StubChannels.publishNoAck[IO](publishSeq, publishSleepDuration))
        amqpClient <- Dispatcher[IO].flatMap { dispatcher =>
          AmqpClient[IO](
            config,
            () => Resource.pure[IO, Channel[IO]](channel),
            Resource.pure[IO, Channel[IO]](channel),
            dispatcher
          )
        }
      } yield TestInfra(amqpClient, channel, publishSeq)
  }

  test("A message publishing should only complete when an ack is returned.") {
    val init = infraInit(config = Config.empty(10.seconds), publishSleepDuration = 5.seconds)
      runAmqpTestInfra(init) { case TestInfra(client, channel, publishSeq) =>
        for {
          pubSeq <- publishSeq.get
          ftr <- IO(client.publisher()(commandBuilder).unsafeToFuture())
          _ <- IO.sleep(3.seconds) >> IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
        } yield ftr.isCompleted shouldBe false
      }
  }

  test("A message publishing should error when a return is returned.") {
    //Publish messag,e wait 3 seconds , handle return to force publish failure, completed fiber should be an error?
    val init = infraInit(config = Config.empty(10.seconds), publishSleepDuration = 3.seconds)
    runAmqpTestInfra(init) { case TestInfra(client, channel, publishSeq) =>
        for {
          pubSeq <- publishSeq.get
          publishFiber <- client.publisher()(commandBuilder).start
          properties = new BasicProperties
          _ = properties.builder().build()
          _ <- IO(Logger(getClass).info("About to handleReturn"))
          _ <- IO(channel.returnListeners.foreach(_.handleReturn(400, "reply", exchange.value, rk.value, properties, message.getBytes)))
          _ <- IO(channel.confirmListeners.foreach(_.handleAck(pubSeq, false)))
          result <- publishFiber.join
        } yield {
          result.isError shouldBe true
        }
      }
  }

  test("A message publishing should timeout if no ack is ever received.") {
    val init = infraInit(config = Config.empty(10.seconds))
    runAmqpTestInfra(init) { case TestInfra(client, channel, _) =>
      for {
        f1 <- client.publisher()(commandBuilder).start
        listeners <- IO(channel.confirmListeners.map(_.asInstanceOf[PendingConfirmListener[IO]]))
        outcome <- f1.join
        pendingConf <- listeners.toList.map(_.pendingConfirmations.get).sequence
      } yield {
        assert(hasOutcomeError[TimeoutException](outcome))
        withClue("Confirm listeners should be popped") {
          pendingConf.flatMap(_.values) shouldBe empty
        }
      }
    }
  }


  test("Multiple messages can be ack when the channel acks multiple messages") {
    runAmqpTestInfra(infraInit(Config.empty(30.seconds))) { case TestInfra(client, channel, _) =>
      for {
        fiber1 <- client.publisher()(commandBuilder).start
        fiber2 <- client.publisher()(commandBuilder).start
        fiber3 <- client.publisher()(commandBuilder).start
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
    val init = infraInit(config = Config.empty(30.seconds), publishSleepDuration = 3.seconds)
    runAmqpTestInfra(init) { case TestInfra(client, channel, _) =>
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
    val init = infraInit(config = Config.empty(10.seconds))
    runAmqpTestInfra(init) { case TestInfra(client, channel, _) =>
      for {
        f1 <- client.publisher()(commandBuilder).start
        f2 <- client.publisher()(commandBuilder).start
        f3 <- client.publisher()(commandBuilder).start
        f4 <- client.publisher()(commandBuilder).start

        _ <- IO(channel.confirmListeners.foreach(_.handleNack(0, false)))
        _ <- IO(channel.confirmListeners.foreach(_.handleNack(1, false))) //nack 1, 2
        _ <- IO(channel.confirmListeners.foreach(_.handleAck(2, false))) // ack 0

        outcome1 <- f1.join
        outcome2 <- f2.join
        outcome3 <- f3.join
        outcome4 <- f4.join
      } yield {

        val outcomes = List(outcome1, outcome2, outcome3, outcome4)
        outcomes.count(_.isSuccess) should ===(1)
        outcomes.count(hasOutcomeError[RuntimeException](_)) should ===(2)
        outcomes.count(hasOutcomeError[TimeoutException](_)) should ===(1)
      }
    }
  }
}
