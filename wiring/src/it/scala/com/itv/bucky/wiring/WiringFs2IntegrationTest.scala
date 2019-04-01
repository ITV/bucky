package com.itv.bucky.wiring
import java.util.UUID

import _root_.fs2.Stream
import cats.effect.IO
import cats.effect.IO.contextShift
import cats.implicits._
import com.itv.bucky.UnmarshalResult.Success
import com.itv.bucky._
import com.itv.bucky.ext.fs2.supersync.SuperSyncSimulator
import com.itv.bucky.fs2._
import org.scalatest.FunSuite

import scala.concurrent.duration._

class WiringFs2IntegrationTest extends FunSuite with WiringIntegrationTest {

  val incoming = new Wiring[String](WiringName("fs2.incoming"))
  val outgoing = new Wiring[String](WiringName("fs2.outgoing"))

  implicit val cs = contextShift(executionContext)

  test("Wirings should publish and consume messages") {
    withApp { fixture =>
      for {
        publishMessage <- incoming.publisher(fixture.client)
        _              <- publishMessage("fs2 publisher test")
      } yield {
        assert(fixture.sink.receivedMessages.size == 1)
        assert(fixture.sink.receivedMessages.head.body.unmarshal[String] == Success(s"Outgoing: fs2 publisher test"))
      }
    }
  }

  case class Fixture(
      client: SuperSyncSimulator,
      sink: StubConsumeHandler[IO, Delivery]
  )

  def createAppStream(client: IOAmqpClient): Stream[IO, _] = {
    val sendOutgoingMessage = outgoing.publisher(client)

    val handleIncoming = incoming.consumer(client) { message =>
      logger.info(s"Forwarding received message to sink: message=$message")
      sendOutgoingMessage(s"Outgoing: $message").map(_ => Ack)
    }

    Stream
      .emit(client)
      .concurrently(handleIncoming)
      .map(_ => client)
  }

  def withApp(fn: Fixture => IO[_]) = {
    val client = new SuperSyncSimulator
    val result = for {
      _      <- createAppStream(client)
      sink   <- outgoing.fs2StubConsumeHandler(client)
      result <- Stream
        .eval(fn(Fixture(client, sink)))
        .attempt
    } yield result

    result.delayBy(2.second)(IO.timer(executionContext)).compile.last.unsafeRunSync() match {
      case None             => fail("Got no result from test. This should not happen")
      case Some(Left(err))  => throw err
      case Some(Right(res)) => res
    }
  }

}
