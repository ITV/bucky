package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky
import com.itv.bucky.{ChannelAmqpPurgeTest, RequeueStrategy, TestFixture}
import org.scalatest.Assertion

import org.scalatest.Matchers._
class IOChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[IO] {

  override def withPublisher(testQueueName: bucky.QueueName,
                             requeueStrategy: RequeueStrategy[IO],
                             shouldDeclare: Boolean)(f: (TestFixture[IO]) => Unit): Unit =
    IntegrationUtils.withPublisher(testQueueName, requeueStrategy, shouldDeclare)(f)

  override def verifySuccess(f: IO[Unit]): Assertion =
    f.unsafeRunSync() should ===(())

}
