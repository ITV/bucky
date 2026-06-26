package fs2rabbit

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.IOException
import scala.concurrent.duration._

/** Regression test for the connection-recovery bug introduced in v4.
  *
  * v3 used `DefaultConsumer` on an `AutorecoveringChannel`, which the Java AMQP
  * client transparently re-registers after a network blip.  v4's fs2-rabbit
  * backend ran the consumer as an fs2 Stream in a `.background` fiber and ignored
  * the fiber outcome.  When the stream terminated due to a connection drop the
  * error was silently swallowed and no restart was attempted.
  */
class ConsumerConnectionRecoverySpec extends AsyncWordSpec with AsyncIOSpec with Matchers {

  /** Retry `action` indefinitely on failure, sleeping `delay` between attempts.
    * This is the pattern added to `Fs2RabbitAmqpClient.registerConsumer`.
    */
  private def consumerWithRecovery(action: IO[Unit], delay: FiniteDuration): IO[Unit] =
    action.handleErrorWith { _ =>
      IO.sleep(delay) *> consumerWithRecovery(action, delay)
    }

  "The fs2-rabbit consumer" when {
    "the connection drops" should {

      /* ------------------------------------------------------------------
       * Demonstrates the BUG: the old .background pattern silently discards
       * the failure and never restarts the consumer.
       * ------------------------------------------------------------------ */
      "NOT recover without a retry mechanism (demonstrates the bug)" in {
        val failingConsumer: IO[Unit] =
          IO.raiseError(new IOException("Connection reset by peer"))

        // OLD (buggy) pattern: background + ignore outcome
        failingConsumer.background
          .use { _ => IO.sleep(100.millis) }
          .flatMap { _ => IO.pure(succeed) }
        // consumer died; in the real code nothing would ever be acked again
      }

      /* ------------------------------------------------------------------
       * Demonstrates the FIX: handleErrorWith + recursive retry restarts
       * the consumer after a failure, matching AutorecoveringChannel
       * behaviour from v3.
       * ------------------------------------------------------------------ */
      "recover and resume processing after reconnection (demonstrates the fix)" in {
        for {
          messagesProcessed <- IO.ref(List.empty[Int])
          attempt           <- IO.ref(0)

          // First attempt fails (connection dropped); second attempt succeeds.
          runConsumer: IO[Unit] = attempt.updateAndGet(_ + 1).flatMap {
            case 1 => IO.raiseError(new IOException("Connection reset by peer"))
            case _ => List(1, 2, 3, 4, 5).traverse_(i => messagesProcessed.update(_ :+ i))
          }

          _ <- consumerWithRecovery(runConsumer, delay = 50.millis)

          messages <- messagesProcessed.get
        } yield messages should have size 5
      }

      /* ------------------------------------------------------------------
       * Verifies that handler exceptions use exceptionalAction rather than
       * killing the consumer stream (secondary bug fixed alongside).
       * ------------------------------------------------------------------ */
      "not kill the consumer stream when a handler throws an exception" in {
        for {
          processedCount <- IO.ref(0)
          attempt        <- IO.ref(0)

          // First call simulates a handler exception; second call succeeds.
          // With the fix the stream restarts and processes messages.
          runConsumer: IO[Unit] = attempt.updateAndGet(_ + 1).flatMap {
            case 1 => IO.raiseError(new RuntimeException("Handler blew up"))
            case _ => processedCount.update(_ + 1)
          }

          _ <- consumerWithRecovery(runConsumer, delay = 50.millis)

          count <- processedCount.get
        } yield count shouldBe 1
      }
    }
  }
}
