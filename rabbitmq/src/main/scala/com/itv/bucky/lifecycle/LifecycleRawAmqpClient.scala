package com.itv.bucky.lifecycle

import com.itv.bucky._
import com.itv.bucky.future.FutureAmqpClient
import com.itv.lifecycle.Lifecycle
import com.rabbitmq.client.{Channel => RabbitChannel}

import scala.concurrent.ExecutionContext
import scala.util.Try

class LifecycleRawAmqpClient(channelFactory: Lifecycle[RabbitChannel])(implicit executionContext: ExecutionContext) extends FutureAmqpClient[Lifecycle](channelFactory) {

  override implicit def B: Monad[Lifecycle] = lifecycleMonad

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = Lifecycle.using(channelFactory)(channel => thunk(ChannelAmqpOps(channel)))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] =  Lifecycle.using(channelFactory) { channel =>
    Channel.estimateMessageCount(channel, queueName)
  }


}
