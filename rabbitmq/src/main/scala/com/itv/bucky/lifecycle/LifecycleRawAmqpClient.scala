package com.itv.bucky.lifecycle

import com.itv.bucky._
import com.itv.bucky.future.FutureAmqpClient
import com.itv.lifecycle.Lifecycle
import com.rabbitmq.client.Channel

import scala.concurrent.ExecutionContext
import scala.util.Try

class LifecycleRawAmqpClient(channelFactory: Lifecycle[Channel])(implicit executionContext: ExecutionContext) extends FutureAmqpClient[Lifecycle](channelFactory) {

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = Lifecycle.using(channelFactory)(channel => thunk(ChannelAmqpOps(channel)))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] =  Lifecycle.using(channelFactory) { channel =>
    Try(Option(channel.basicGet(queueName.value, false)).fold(0)(_.getMessageCount + 1))
  }

}
