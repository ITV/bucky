package com.itv.bucky.wiring
import com.itv.bucky.UnmarshalResult.Success
import com.itv.bucky._
import com.itv.bucky.future.futureMonad
import com.itv.bucky.lifecycle.AmqpClientLifecycle
import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class WiringLifecycleIntegrationTest extends FunSuite with WiringIntegrationTest {

  val incoming = new Wiring[String](WiringName("lifecycle.incoming"))
  val outgoing = new Wiring[String](WiringName("lifecycle.outgoing"))

  implicit val monad: MonadError[Future, Throwable] = futureMonad

  test("Wirings should publish and consume messages") {
    withApp { fixture =>
      for {
        _ <- fixture.publisher("Lifecycle publisher test message")
      } yield
        eventually {
          assert(fixture.sink.receivedMessages.size == 1)
          assert(fixture.sink.receivedMessages.head.body.unmarshal[String] == Success("Outgoing: Lifecycle publisher test message"))
        }
    }
  }

  case class Fixture(
      client: AmqpClient[Lifecycle, Future, scala.Throwable, Unit],
      sink: StubConsumeHandler[Future, Delivery],
      publisher: String => Future[Unit]
  )

  def createApp(client: AmqpClient[Lifecycle, Future, Throwable, Unit]): Lifecycle[_] =
    for {
      sendOutgoingMessage <- outgoing.publisher(client)
      _ <- incoming.consumer(client) { message =>
        logger.info(s"Forwarding received message to sink: message=$message")
        sendOutgoingMessage(s"Outgoing: $message").map(_ => Ack)
      }
    } yield ()

  def withApp(fn: Fixture => Future[_]) = {
    val result = for {
      client    <- AmqpClientLifecycle(amqpConfig)
      _         <- createApp(client)
      sink      <- outgoing.lifecycleStubConsumeHandler(client)
      publisher <- incoming.publisher(client)
      result    <- NoOpLifecycle(Await.result(fn(Fixture(client, sink, publisher)), Duration.Inf))
    } yield result

    result.runUntilJvmShutdown()

  }
}
