package com.itv.bucky.fs2

import cats.effect.{IO, Timer}
import _root_.fs2._
import cats.effect.concurrent.Ref
import cats.instances.future
import com.itv.bucky._
import com.itv.bucky.ext.fs2._
import org.scalatest.{Assertion, FlatSpec}
import org.scalatest.concurrent.Eventually._
import org.scalatest.Matchers._
import org.scalactic.TypeCheckedTripleEquals

import scala.collection.mutable.ListBuffer
import examples._

import scala.concurrent.{ExecutionContext, Future}

class Fs2AmqpSimulatorTest extends FlatSpec with TypeCheckedTripleEquals {
  import UnmarshalResultOps._
  import Fs2AmqpSimulatorTest._
  import App._
  import cats.syntax.traverse._
  import cats.instances.list._

  it should "send and consume message using simulator" in {
    withApp { app =>
      def publishSource(message: String) =
        app.amqpClient.publish(RmqConfig.Source.stringPublishCommandBuilder.toPublishCommand(message))

      val expectedFooMessages = (1 to 5).map(i => s"foo-$i").toList
      val expectedBarMessages = (5 to 10).map(i => s"bar-$i").toList
      for {
        _ <- waitForConsumer(app)

        _ <- expectedFooMessages.traverse(publishSource)

        _ <- expectedBarMessages.traverse(publishSource)

        _ <- app.amqpClient.waitForMessagesToBeProcessed()

        _ <- app.bar.map(_ should ===(expectedBarMessages))
      } yield {
        app.targetMessages should have size expectedFooMessages.size

        app.targetMessages.map(_.envelope.routingKey).toSet should ===(Set(RmqConfig.Target.routingKey))
        app.targetMessages.map(_.envelope.exchangeName).toSet should ===(Set(RmqConfig.Target.exchangeName))
        app.targetMessages.map(_.body.unmarshal[String].success).toSet should ===(expectedFooMessages.toSet)

      }
    }
  }

  private def waitForConsumer(app: Ports) =
    IO {
      eventually {
        app.amqpClient.existsConsumer(RmqConfig.Source.queueName) shouldBe true
      }
    }
}

object Fs2AmqpSimulatorTest {
  import App._

  import com.itv.bucky.future.SameThreadExecutionContext.implicitly
  implicit val futureMonad: MonadError[Future, Throwable] = com.itv.bucky.future.futureMonad

  implicit val IOTimer: Timer[IO] = IO.timer(implicitly)

  case class Ports(amqpClient: Fs2AmqpSimulator, targetMessages: ListBuffer[Delivery], bar: IO[List[String]])

  def withApp(f: Ports => IO[Assertion]): Unit =
    withSimulator(RmqConfig.all)(buildPorts)(f)

  def buildPorts(amqpClient: Fs2AmqpSimulator) =
    for {
      ref <- Stream.eval(Ref.of[IO, List[String]](List.empty))
      app = App(amqpClient, new Bar {
        override def add(message: String): IO[Unit] = ref.modify(messages => messages.:+(message) -> messages.:+(message) ).map(_ => ())
      })

      messages <- amqpClient.consume(RmqConfig.Target.exchangeName,
        RmqConfig.Target.routingKey,
        RmqConfig.Target.queueName)

      ports <- Stream
        .eval(IO(Ports(amqpClient, messages, ref.get)))
        .concurrently(app.amqp)
    } yield ports

}
