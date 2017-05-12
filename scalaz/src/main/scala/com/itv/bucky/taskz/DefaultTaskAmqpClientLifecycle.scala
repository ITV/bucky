package com.itv.bucky.taskz

import com.itv.bucky.{AmqpClientConfig, Channel}
import com.itv.lifecycle.VanillaLifecycle


case class DefaultTaskAmqpClientLifecycle(config: AmqpClientConfig) extends VanillaLifecycle[TaskAmqpClient]{
  override def start(): TaskAmqpClient = TaskAmqpClient(config)

  override def shutdown(instance: TaskAmqpClient): Unit = Channel.closeAll(instance.channel)
}
