package com.itv.bucky

import cats.effect.IO
import com.itv.bucky.consume.{Ack, Delivery}
import com.itv.bucky.wiring._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer
import cats.implicits._
import org.scalatest.Matchers._

class WiringPublishConsumeTest extends FunSuite with StrictLogging {

  import SuperTest._

  val incoming = new Wiring[String](WiringName("fs2.incoming"))
  val outgoing = new Wiring[String](WiringName("fs2.outgoing"))

  val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  implicit val cs = IO.contextShift(executionContext)

  def accHandler: (ListBuffer[String], Handler[IO, String]) = {
    val acc: ListBuffer[String] = ListBuffer.empty[String]
    (acc, (delivery: String) => IO.delay {
      acc.append(delivery)
      Ack
    })
  }

  test("Wirings should publish and consume messages") {
    withApp { fixture =>
      for {
        publishMessage <- incoming.publisher(fixture.client)
        _              <- publishMessage("fs2 publisher test")
      } yield {
        fixture.sink should have size 1
        fixture.sink.head shouldBe "Outgoing: fs2 publisher test"
      }
    }
  }

  case class Fixture(
                      client: AmqpClient[IO],
                      sink: ListBuffer[String]
                    )

  def withApp[A](fn: Fixture => IO[A]): Unit = {
    withDefaultClient() { client =>
      val (acc, handler) = accHandler

      for {
        sendOutgoingMessage <- outgoing.publisher(client)
        _ <- incoming.registerConsumer(client) { message =>
          logger.info(s"Forwarding received message to sink: message=$message")
          sendOutgoingMessage(s"Outgoing: $message").map(_ => Ack)
        }
        _ <- outgoing.registerConsumer(client)(handler)
        fixture = Fixture(client, acc)
        _ <- fn(fixture)
      }
        yield ()
    }
  }

}

