package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky
import com.itv.bucky._
import org.scalatest.{Assertion, FunSuite}
import org.scalatest.Matchers._

class IOEstimatedMessageCountTest extends FunSuite with EstimatedMessageCountTest[IO] {
  import cats.implicits._
  import _root_.fs2.async
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  override def runAll(tasks: Seq[IO[Unit]]): Unit =
    async.parallelSequence(tasks.toList).unsafeRunSync()

  override def withPublisher(testQueueName: bucky.QueueName,
                             requeueStrategy: RequeueStrategy[IO],
                             shouldDeclare: Boolean)(f: (TestFixture[IO]) => Unit): Unit =
    IntegrationUtils.withPublisher(testQueueName, requeueStrategy, shouldDeclare)(f)

  override def verifySuccess(f: IO[Unit]): Assertion =
    f.unsafeRunSync() should ===(())

}
