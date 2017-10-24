package com.itv.bucky.taskz

import com.itv.bucky
import com.itv.bucky.suite.{EstimatedMessageCountTest, RequeueStrategy, TestFixture}

import scalaz.concurrent.Task

class TaskEstimatedMessageCountTest extends EstimatedMessageCountTest[Task] with TaskEffectVerification {

  override def runAll(tasks: Seq[Task[Unit]]): Unit = Task.gatherUnordered(tasks).unsafePerformSync

  override def withPublisher(testQueueName: bucky.QueueName,
                             requeueStrategy: RequeueStrategy[Task],
                             shouldDeclare: Boolean)(f: (TestFixture[Task]) => Unit): Unit =
    IntegrationUtils.withPublisher(testQueueName, requeueStrategy, shouldDeclare)(f)

}
