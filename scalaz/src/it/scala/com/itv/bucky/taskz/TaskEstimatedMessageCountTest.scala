package com.itv.bucky.taskz

import com.itv.bucky
import com.itv.bucky.template.{EstimatedMessageCountTest, RequeueStrategy, TestFixture}
import org.scalatest.{Assertion, FunSuite}

import scalaz.\/-
import scalaz.concurrent.Task
import org.scalatest.Matchers._

class TaskEstimatedMessageCountTest extends FunSuite with EstimatedMessageCountTest[Task] {

  override def runAll(tasks: Seq[Task[Unit]]): Unit = Task.gatherUnordered(tasks).unsafePerformSync

  override def withPublisher(testQueueName: bucky.QueueName,
                             requeueStrategy: RequeueStrategy[Task],
                             shouldDeclare: Boolean)(f: (TestFixture[Task]) => Unit): Unit =
    IntegrationUtils.withPublisher(testQueueName, requeueStrategy, shouldDeclare)(f)

  override def verifySuccess(f: Task[Unit]): Assertion =
    f.unsafePerformSyncAttempt should ===(\/-(()))

}
