package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._

import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import org.scalatest.Matchers._

class IOPublisherIntegrationTest extends PublisherIntegrationTest[IO, Throwable] {
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(1.seconds, 100.millis)

  override def verifySuccess(f: IO[Unit]): Assertion = eventually {
    f.unsafeRunSync() should ===(())
  }

  override def withPublisherAndConsumer(queueName: QueueName, requeueStrategy: RequeueStrategy[IO])(
      f: (TestFixture[IO]) => Unit): Unit =
    IntegrationUtils.withPublisherAndConsumer(queueName, requeueStrategy)(f)

  override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError
}
