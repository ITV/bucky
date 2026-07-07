package fs2rabbit

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import com.itv.bucky.{AmqpClientConfig, Envelope, ExchangeName, Payload, QueueName, RoutingKey, consume, publish}
import com.itv.bucky.backend.fs2rabbit.{AmqpClientConnectionManager, Fs2RabbitAmqpClient}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpProperties, DeliveryTag}
import fs2.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.IOException
import scala.concurrent.duration._

/** Regression tests for the connection-recovery bug introduced in v4.
  *
  * v3 used `DefaultConsumer` on an `AutorecoveringChannel`, which the Java AMQP
  * client transparently re-registers after a network blip. v4's fs2-rabbit
  * backend ran the consumer as an fs2 Stream in a `.background` fiber and ignored
  * the fiber outcome. When the stream terminated due to a connection drop the
  * error was silently swallowed and no restart was attempted.
  *
  * These tests exercise the actual `Fs2RabbitAmqpClient.registerConsumer` method
  * by subclassing it and overriding `acquireConsumerStream` with controlled
  * in-memory streams — no real RabbitMQ connection is required.
  */
class ConsumerConnectionRecoverySpec extends AsyncWordSpec with AsyncIOSpec with Matchers {

  /** Build a minimal `AmqpEnvelope[consume.Delivery]` suitable for use in tests. */
  private def makeEnvelope(tag: Long): model.AmqpEnvelope[consume.Delivery] =
    AmqpEnvelope(
      DeliveryTag(tag),
      consume.Delivery(
        Payload("test".getBytes),
        consume.ConsumerTag("ctag"),
        Envelope(tag, redeliver = false, ExchangeName("ex"), RoutingKey("rk")),
        publish.MessageProperties.minimalBasic
      ),
      AmqpProperties.empty,
      model.ExchangeName("ex"),
      model.RoutingKey("rk"),
      redelivered = false
    )

  /** Config with a short recovery interval so tests don't wait 3 seconds. */
  private val testConfig: AmqpClientConfig =
    AmqpClientConfig("localhost", 5672, "guest", "guest", networkRecoveryInterval = Some(50.millis))

  /** Create a test double for `Fs2RabbitAmqpClient` that serves controlled
    * `(acker, stream)` pairs from `streamsRef` in order.  No real AMQP
    * connection is needed: the overridden `acquireConsumerStream` never calls
    * `client` or `connection`.
    */
  private def makeTestClient(
      streamsRef: Ref[IO, List[(model.AckResult => IO[Unit], Stream[IO, model.AmqpEnvelope[consume.Delivery]])]]
  ): Fs2RabbitAmqpClient[IO] =
    new Fs2RabbitAmqpClient[IO](
      testConfig,
      null.asInstanceOf[RabbitClient[IO]],
      null.asInstanceOf[model.AMQPConnection],
      null.asInstanceOf[model.AMQPChannel],
      null.asInstanceOf[AmqpClientConnectionManager[IO]]
    ) {
      override protected def acquireConsumerStream(queueName: QueueName) =
        Resource.eval(streamsRef.modify {
          case head :: tail => (tail, head)
          case Nil          => sys.error("No more test streams available")
        })
    }

  "Fs2RabbitAmqpClient.registerConsumer" when {

    "the consumer stream fails (simulated connection drop)" should {

      "retry and resume processing messages after recovery" in {
        // stream1 emits one message then fails, simulating a dropped connection
        val stream1 = Stream.emit(makeEnvelope(1L)) ++ Stream.raiseError[IO](new IOException("Connection reset by peer"))
        val acker1  = (_: model.AckResult) => IO.unit
        // stream2 emits one more message then completes normally
        val stream2 = Stream.emit(makeEnvelope(2L))
        val acker2  = (_: model.AckResult) => IO.unit

        for {
          processedTags <- IO.ref(List.empty[Long])
          done          <- IO.deferred[Unit]
          streamsRef    <- IO.ref(List((acker1, stream1), (acker2, stream2)))
          client        = makeTestClient(streamsRef)
          handler       = (delivery: consume.Delivery) =>
            processedTags.update(_ :+ delivery.envelope.deliveryTag) *>
              processedTags.get.flatMap(tags => if (tags.size >= 2) done.complete(()).void else IO.unit) *>
              IO.pure(consume.Ack)
          _ <- client
            .registerConsumer(QueueName("test"), handler, consume.DeadLetter, 10, 500.millis, 100.millis)
            .use(_ => done.get.timeout(5.seconds))
          tags <- processedTags.get
        } yield {
          tags should contain(1L)
          tags should contain(2L)
        }
      }
    }

    "a handler throws an exception" should {

      "use exceptionalAction rather than killing the consumer stream" in {
        for {
          ackerResults <- IO.ref(List.empty[model.AckResult])
          done         <- IO.deferred[Unit]
          acker        = (result: model.AckResult) =>
            ackerResults.update(_ :+ result) *>
              ackerResults.get.flatMap(rs => if (rs.size >= 2) done.complete(()).void else IO.unit)
          stream     = Stream.emits(List(makeEnvelope(1L), makeEnvelope(2L))).covary[IO]
          streamsRef <- IO.ref(List((acker, stream)))
          client     = makeTestClient(streamsRef)
          // Handler throws for the first message; succeeds (Ack) for the second
          handler = (delivery: consume.Delivery) =>
            if (delivery.envelope.deliveryTag == 1L)
              IO.raiseError[consume.ConsumeAction](new RuntimeException("handler explosion"))
            else
              IO.pure(consume.Ack)
          _ <- client
            .registerConsumer(QueueName("test"), handler, consume.DeadLetter, 10, 500.millis, 100.millis)
            .use(_ => done.get.timeout(5.seconds))
          results <- ackerResults.get
        } yield {
          // tag 1: handler threw → exceptionalAction (DeadLetter) → NAck
          results should contain(model.AckResult.NAck(DeliveryTag(1L)))
          // tag 2: handler returned Ack → Ack
          results should contain(model.AckResult.Ack(DeliveryTag(2L)))
        }
      }
    }

    "messages are processed normally" should {

      "route Ack / DeadLetter / RequeueImmediately to the correct AckResult" in {
        for {
          ackerResults <- IO.ref(List.empty[model.AckResult])
          done         <- IO.deferred[Unit]
          acker        = (result: model.AckResult) =>
            ackerResults.update(_ :+ result) *>
              ackerResults.get.flatMap(rs => if (rs.size >= 3) done.complete(()).void else IO.unit)
          stream     = Stream.emits(List(makeEnvelope(1L), makeEnvelope(2L), makeEnvelope(3L))).covary[IO]
          streamsRef <- IO.ref(List((acker, stream)))
          client     = makeTestClient(streamsRef)
          handler = (delivery: consume.Delivery) =>
            delivery.envelope.deliveryTag match {
              case 1L => IO.pure(consume.Ack)
              case 2L => IO.pure(consume.DeadLetter)
              case _  => IO.pure(consume.RequeueImmediately)
            }
          _ <- client
            .registerConsumer(QueueName("test"), handler, consume.DeadLetter, 10, 500.millis, 100.millis)
            .use(_ => done.get.timeout(5.seconds))
          results <- ackerResults.get
        } yield {
          results should contain(model.AckResult.Ack(DeliveryTag(1L)))
          results should contain(model.AckResult.NAck(DeliveryTag(2L)))
          results should contain(model.AckResult.Reject(DeliveryTag(3L)))
        }
      }
    }
  }
}
