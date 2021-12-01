package com.itv.bucky.test

import cats.effect.{IO, Resource}
import com.itv.bucky.AmqpClient
import com.itv.bucky.consume.Ack
import com.itv.bucky.test.stubs.RecordingHandler
import com.itv.bucky.wiring._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.funsuite.{AnyFunSuite, AsyncFunSuite}
import org.scalatest.matchers.should.Matchers._
import cats.effect.unsafe.IORuntime

class WiringPublishConsumeTest extends AsyncFunSuite with GlobalAsyncIOSpec with IOAmqpClientTest with StrictLogging {

  val incoming = new Wiring[String](WiringName("fs2.incoming"))
  val outgoing = new Wiring[String](WiringName("fs2.outgoing"))

  override implicit val ioRuntime: IORuntime = cats.effect.unsafe.implicits.global

  test("Wirings should publish and consume messages") {
    withApp { fixture =>
      for {
        publishMessage <- incoming.publisher(fixture.client)
        _              <- publishMessage("fs2 publisher test")
      } yield {
        fixture.sink.receivedMessages should have size 1
        fixture.sink.receivedMessages.head shouldBe "Outgoing: fs2 publisher test"
      }
    }
  }

  case class Fixture(
      client: AmqpClient[IO],
      sink: RecordingHandler[IO, String]
  )

  def withApp[A](fn: Fixture => IO[A]): IO[Unit] =
    runAmqpTest { client =>
      val handler = StubHandlers.ackHandler[IO, String]
      (for {
        sendOutgoingMessage <- Resource.eval(outgoing.publisher(client))
        _ <- incoming.registerConsumer(client) { message =>
          logger.info(s"Forwarding received message to sink: message=$message")
          sendOutgoingMessage(s"Outgoing: $message").map(_ => Ack)
        }
        _ <- outgoing.registerConsumer(client)(handler)

      } yield ()).use { _ =>
        for {
          fixture <- IO(Fixture(client, handler))
          _       <- fn(fixture)
        } yield ()
      }

    }

}
