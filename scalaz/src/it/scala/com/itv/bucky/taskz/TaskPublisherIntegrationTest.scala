package com.itv.bucky.taskz

import com.itv.bucky._
import org.scalatest.Assertion
import org.scalatest.Matchers._

import scalaz.\/-
import scalaz.concurrent.Task


class TaskPublisherIntegrationTest extends PublisherIntegrationTest[Task, Throwable] {

  override def verifySuccess(f: Task[Unit]): Assertion = f.unsafePerformSyncAttempt should ===(\/-(()))

  override def withPublisherAndConsumer(queueName: QueueName, requeueStrategy: RequeueStrategy[Task])
                                       (f: (TestFixture[Task]) => Unit): Unit =
    IntegrationUtils.withPublisherAndConsumer(queueName, requeueStrategy)(f)

  override implicit def effectMonad: MonadError[Task, Throwable] = TaskExt.taskMonad
}
