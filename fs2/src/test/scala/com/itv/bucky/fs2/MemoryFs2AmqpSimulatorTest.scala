package com.itv.bucky.fs2

import cats.effect.{ContextShift, IO, Timer}
import _root_.fs2._
import cats.effect.concurrent.Ref
import com.itv.bucky._
import com.itv.bucky.ext.fs2._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Assertion, FlatSpec}
import org.scalatest.Matchers._
import org.scalactic.TypeCheckedTripleEquals

import scala.collection.mutable.ListBuffer
import examples._

import scala.concurrent.Future

class MemoryFs2AmqpSimulatorTest extends FlatSpec with TypeCheckedTripleEquals with StrictLogging {
  import UnmarshalResultOps._
  import MemoryFs2AmqpSimulatorTest._
  import App._
  import cats.syntax.traverse._
  import cats.instances.list._
  import scala.concurrent.duration._

  it should "send and consume one message using simulator" in {
    withApp { app =>
      app.amqpClient
        .publishAndWait(RmqConfig.Source.stringPublishCommandBuilder.toPublishCommand("bar-1"), 100.millis)
        .map(_ should ===(Ack.result))
    }
  }

  it should "send and republish one message using simulator" in {
    withApp { app =>
      for {
        p      <- app.amqpClient.publish(RmqConfig.Source.stringPublishCommandBuilder.toPublishCommand("foo-1"))
        result <- p.get
      } yield {

        result should ===(Ack.result)
      }
    }
  }

  it should "send and consume multiple messages using simulator" in {
    withApp { app =>
      def publishSource(message: String) =
        app.amqpClient.publish(RmqConfig.Source.stringPublishCommandBuilder.toPublishCommand(message))

      val expectedFooMessages = (1 to 5).map(i => s"foo-$i").toList
      val expectedBarMessages = (5 to 10).map(i => s"bar-$i").toSet
      for {

        _ <- expectedFooMessages.traverse(publishSource)

        _ <- expectedBarMessages.toList.traverse(publishSource)
        _ <- app.amqpClient.waitForMessagesToBeProcessed(1.second)

        _ <- app.bar.map(_.toSet should ===(expectedBarMessages))
      } yield {

        app.targetMessages should have size expectedFooMessages.size

        app.targetMessages.map(_.envelope.routingKey).toSet should ===(Set(RmqConfig.Target.routingKey))
        app.targetMessages.map(_.envelope.exchangeName).toSet should ===(Set(RmqConfig.Target.exchangeName))
        app.targetMessages.map(_.body.unmarshal[String].success).toSet should ===(expectedFooMessages.toSet)

      }
    }
  }

  it should "return timeout when handler it is not able to finish its actions" in {
    withApp { app =>
      val publishCommand: PublishCommand = RmqConfig.Source.stringPublishCommandBuilder.toPublishCommand("no_end-1")
      val timeout                        = 50.millis
      for {
        _      <- app.amqpClient.publish(publishCommand)
        result <- app.amqpClient.waitForMessagesToBeProcessed(timeout)
      } yield result should ===(List(publishCommand.timeout(timeout)))
    }
  }

  it should "return explain why it it is not able to finish when there is not binding" in {
    withApp { app =>
      val publishCommand: PublishCommand =
        RmqConfig.InvalidBinding.stringPublishCommandBuilder.toPublishCommand("no_binding")
      app.amqpClient
        .publishAndWait(publishCommand, 100.millis)
        .map(_ should ===(publishCommand.notBindingFound(MemoryFs2AmqpSimulatorTest.retryPolicy)))
    }
  }

  it should "return explain why it it is not able to finish when there is not consumer" in {
    withApp { app =>
      val publishCommand: PublishCommand =
        RmqConfig.NoConsumer.stringPublishCommandBuilder.toPublishCommand("no_consumer")
      app.amqpClient
        .publishAndWait(publishCommand, 100.millis)
        .map(_ should ===(publishCommand.notConsumerFound(MemoryFs2AmqpSimulatorTest.retryPolicy)))
    }
  }

}

object MemoryFs2AmqpSimulatorTest {
  import App._
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly
  import scala.concurrent.duration._
  implicit val futureMonad: MonadError[Future, Throwable] = com.itv.bucky.future.futureMonad

  implicit val IOTimer: Timer[IO] = IO.timer(implicitly)

  implicit val contextShift: ContextShift[IO] = IO.contextShift(implicitly)

  case class Ports(amqpClient: MemoryAmqpSimulator[IO], targetMessages: ListBuffer[Delivery], bar: IO[List[String]])

  val retryPolicy = MemoryAmqpSimulator.RetryPolicy(5, 10.millis)

  def withApp(f: Ports => IO[Assertion]): Unit =
    withSafeSimulator(RmqConfig.all, MemoryAmqpSimulator.Config(retryPolicy))(buildPorts)(f)
      .compile
      .drain
      .unsafeRunSync()

  def buildPorts(amqpClient: MemoryAmqpSimulator[IO]) =
    for {
      ref <- Stream.eval(Ref.of[IO, List[String]](List.empty))

      app = App(amqpClient, new Bar {
        override def add(message: String): IO[Unit] = ref.modify(messages => messages.:+(message) -> messages.:+(message)).map(_ => ())
      })

      messages <- amqpClient.consume(RmqConfig.Target.exchangeName,
        RmqConfig.Target.routingKey,
        RmqConfig.Target.queueName)

      ports <- Stream
        .eval(IO(Ports(amqpClient, messages, ref.get)))
        .concurrently(app.amqp)
    } yield ports

}
