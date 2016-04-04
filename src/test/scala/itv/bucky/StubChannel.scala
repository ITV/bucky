package itv.bucky

import com.google.common.util.concurrent.MoreExecutors
import com.rabbitmq.client.impl.AMQImpl.Basic.ConsumeOk
import com.rabbitmq.client.impl.AMQImpl.Confirm.SelectOk
import com.rabbitmq.client.impl.{AMQCommand, ChannelN, ConsumerWorkService}
import com.rabbitmq.client.{MessageProperties, AMQP, Method}
import itv.utils.Blob

import scala.collection.mutable.ListBuffer

class StubChannel extends ChannelN(null, 0, new ConsumerWorkService(MoreExecutors.sameThreadExecutor(), null)) {

  val transmittedCommands: ListBuffer[Method] = ListBuffer.empty
  val consumers: ListBuffer[AMQP.Basic.Consume] = ListBuffer.empty

  override def quiescingTransmit(c: AMQCommand): Unit = {
    val method = c.getMethod
    transmittedCommands += method
    method match {
      case _: AMQP.Confirm.Select =>
        replyWith(new SelectOk())
      case c: AMQP.Basic.Consume =>
        consumers += c
        replyWith(new ConsumeOk(c.getConsumerTag))
      case _: AMQP.Basic.Publish =>
        ()
    }
  }

  def replyWith(method: Method): Unit = {
    handleCompleteInboundCommand(new AMQCommand(method))
  }

  def deliver(delivery: AMQP.Basic.Deliver, body: Blob, properties: AMQP.BasicProperties = MessageProperties.BASIC): Unit = {
    handleCompleteInboundCommand(new AMQCommand(delivery, properties, body.content))
  }
}
