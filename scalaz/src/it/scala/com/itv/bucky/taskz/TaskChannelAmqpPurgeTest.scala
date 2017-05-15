package com.itv.bucky.taskz

import com.itv.bucky
import com.itv.bucky.{ChannelAmqpPurgeTest, RequeueStrategy, TestFixture}
import org.scalatest.Assertion

import scalaz.\/-
import scalaz.concurrent.Task
import org.scalatest.Matchers._

class TaskChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[Task] {
  override def withPublisher(testQueueName: bucky.QueueName, requeueStrategy: RequeueStrategy[Task], shouldDeclare: Boolean)
                            (f: (TestFixture[Task]) => Unit): Unit = IntegrationUtils.withPublisher(testQueueName, requeueStrategy, shouldDeclare)(f)

  override def verifySuccess(f: Task[Unit]): Assertion = f.unsafePerformSyncAttempt should ===(\/-(()))

}