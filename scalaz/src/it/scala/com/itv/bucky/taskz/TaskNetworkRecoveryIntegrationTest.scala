package com.itv.bucky.taskz

import com.itv.bucky.Monad.Id
import com.itv.bucky.{AmqpClient, AmqpClientConfig}
import com.itv.bucky.suite.NetworkRecoveryIntegrationTest
import com.itv.bucky.taskz.AbstractTaskAmqpClient.TaskConsumer
import com.itv.lifecycle.Lifecycle

import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scalaz.concurrent.Task

class TaskNetworkRecoveryIntegrationTest
    extends NetworkRecoveryIntegrationTest[Task, Throwable, TaskConsumer]
    with TaskEffectVerification
    with TaskMonadEffect {

  override def schedule(f: => Unit, duration: FiniteDuration): Unit = {
    logger.info(s"Schedule the task to be done in $duration")
    Task
      .schedule(f, duration)
      .unsafePerformAsync(_ => ())
  }

  override def buildLifecycle(config: AmqpClientConfig): Lifecycle[AmqpClient[Id, Task, Throwable, TaskConsumer]] =
    DefaultTaskAmqpClientLifecycle(config)

  override def defaultAmqpClientConfig: AmqpClientConfig = IntegrationUtils.config

  override def executeConsumer(c: TaskConsumer): Unit = c.run.unsafePerformAsync(_ => ())
}
