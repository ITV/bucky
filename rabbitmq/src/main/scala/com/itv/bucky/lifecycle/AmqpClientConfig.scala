package com.itv.bucky.lifecycle

import com.itv.bucky._
import com.itv.bucky.decl.{Declaration, DeclarationExecutor}
import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
import com.rabbitmq.client.{Channel, Connection}
import com.typesafe.scalalogging.StrictLogging

case class AmqpClientLifecycle(config: AmqpClientConfig) extends Lifecycle[AmqpClient[Lifecycle]] with StrictLogging {

  override type ServiceInstance = Connection

  override def start(): Connection = {
    RabbitConnection(config)
  }

  override def unwrap(instance: Connection): AmqpClient[Lifecycle] = new RawAmqpClient(AmqpChannelLifecycle(instance))

  override def shutdown(instance: Connection): Unit =
    if (instance.isOpen) {
      instance.close()
    }

}


case class AmqpChannelLifecycle(connection: Connection) extends VanillaLifecycle[Channel] with StrictLogging {
  override def start(): Channel =
    RabbitChannel(connection)



  override def shutdown(channel: Channel): Unit = {
    if (connection.isOpen) {
      channel.close()
    }
  }
}


import scala.concurrent.duration._
import scala.language.higherKinds

case class DeclarationLifecycle[M[_]](declarations: Iterable[Declaration], client: AmqpClient[M], timeout: FiniteDuration = 5.seconds) extends VanillaLifecycle[Unit]{

  def start(): Unit = DeclarationExecutor(declarations, client, timeout)

  override def shutdown(instance: Unit): Unit = ()

}