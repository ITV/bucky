package itv.bucky

import java.io.IOException

import com.google.common.util.concurrent.MoreExecutors
import com.rabbitmq.client.impl.AMQImpl.Basic.ConsumeOk
import com.rabbitmq.client.impl.AMQImpl.Confirm.SelectOk
import com.rabbitmq.client.impl.{AMQCommand, ChannelN, ConsumerWorkService}
import com.rabbitmq.client.{AMQP, Method, MessageProperties => RMessageProperties}

import scala.collection.mutable.ListBuffer

class StubChannel extends ChannelN(null, 0, new ConsumerWorkService(MoreExecutors.newDirectExecutorService(), null)) {

  val transmittedCommands: ListBuffer[Method] = ListBuffer.empty
  val consumers: ListBuffer[AMQP.Basic.Consume] = ListBuffer.empty
  var setPrefetchCount = 0

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

  override def basicQos(prefetchCount: Int): Unit = {
    setPrefetchCount = prefetchCount
  }

  def replyWith(method: Method): Unit = {
    handleCompleteInboundCommand(new AMQCommand(method))
  }

  def deliver(delivery: AMQP.Basic.Deliver, body: Payload, properties: AMQP.BasicProperties = RMessageProperties.BASIC): Unit = {
    handleCompleteInboundCommand(new AMQCommand(delivery, properties, body.value))
  }
}
