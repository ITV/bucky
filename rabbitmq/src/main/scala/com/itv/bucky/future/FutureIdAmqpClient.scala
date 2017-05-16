package com.itv.bucky.future

import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.rabbitmq.client.{Channel => RabbitChannel}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

case class FutureIdAmqpClient(channel: Id[RabbitChannel])(implicit executionContext: ExecutionContext)
  extends FutureAmqpClient[Id](channel)(executionContext) {

  override implicit def B: Monad[Id] = Monad.idMonad

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] =
    Channel.estimateMessageCount(channel, queueName)


}


object FutureIdAmqpClient {

  import Monad.toMonad

  def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): Id[FutureIdAmqpClient] = {
    Connection(config).flatMap(
      Channel(_)
    ).flatMap(
      FutureIdAmqpClient(_)
    )
  }

}