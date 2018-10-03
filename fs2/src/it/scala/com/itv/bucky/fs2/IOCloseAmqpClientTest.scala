package com.itv.bucky.fs2
import com.itv.bucky.fs2.utils.{IOEffectVerification, IOPublisherBaseTest}
import org.scalatest.FunSuite
import _root_.fs2._
import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky.{AmqpClient, QueueName}
import com.itv.bucky.decl.Queue
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class IOCloseAmqpClientTest extends FunSuite with IOPublisherBaseTest with IOEffectVerification {

  test("close connection should not able to execute further request") {
    val name = QueueName(s"foo-${Random.nextLong()}")
    closeableClientFrom(utils.config, List(Queue(name)))
      .flatMap {
        def countMessages(client: AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]): IO[Int] =
          IO { client.estimatedMessageCount(name).get }

        closeableClient =>
          Stream
            .eval(countMessages(closeableClient.client).map(_ should ===(0)))
            .evalMap(_ => closeableClient.close)
            .evalMap(_ =>
              countMessages(closeableClient.client).map(_ =>
                fail(s"It should not be able to execute because stream is closed")))
      }
      .compile
      .drain
      .unsafeRunSync()
    logger.info(s"The program can do more stuff but not the client")
    1 should ===(1)
  }
}
