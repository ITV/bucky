package com.itv.bucky.taskz

import java.util.concurrent.ExecutorService

import com.itv.bucky.AmqpClientConfig
import com.itv.lifecycle.VanillaLifecycle
import com.typesafe.scalalogging.StrictLogging

case class DefaultTaskAmqpClientLifecycle(config: AmqpClientConfig)(implicit pool: ExecutorService)
    extends VanillaLifecycle[TaskAmqpClient]
    with StrictLogging {
  override def start(): TaskAmqpClient = TaskAmqpClient.fromConfig(config)

  override def shutdown(instance: TaskAmqpClient): Unit =
    TaskAmqpClient.closeAll(instance).unsafePerformSync

}
