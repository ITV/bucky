package com.itv.bucky.future

import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.rabbitmq.client.Channel

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

case class FutureIdAmqpClient(channel: Id[Channel])(implicit executionContext: ExecutionContext) extends FutureAmqpClient[Id](channel)(Monad.idMonad, executionContext) {
  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] =
    IdChannel.estimateMessageCount(channel, queueName)
}


object FutureIdAmqpClient {

  import Monad.toMonad

  def apply(config: AmqpClientConfig)(implicit executionContext: ExecutionContext): Id[FutureIdAmqpClient] = {
    IdConnection(config).flatMap(
      IdChannel(_)
    ).flatMap(
      FutureIdAmqpClient(_)
    )
  }

}