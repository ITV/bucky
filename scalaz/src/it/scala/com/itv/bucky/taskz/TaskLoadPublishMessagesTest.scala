package com.itv.bucky.taskz

import com.itv.bucky.suite.LoadPublishMessagesTest
import scalaz.concurrent.Task

class TaskLoadPublishMessagesTest
  extends LoadPublishMessagesTest[Task]
    with TaskPublisherConsumerBaseTest
    with TaskEffectVerification
    with TaskMonadEffect
     {
    //override val numberRequestInParallel = 100

  override def sequence[A](list: Seq[Task[A]]): Task[Seq[A]] =
    Task.gatherUnordered(list)
}