package com.itv.bucky

import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
import com.rabbitmq.client.{Channel, Connection}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

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
/**
  * AmqpClient configuration. Acts as a lifecycle factory for AmqpClient.
  */
case class AmqpClientConfig(
                             host: String,
                             port: Int,
                             username: String,
                             password: String,
                             networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds),
                             virtualHost: Option[String] = None)

case class AmqpChannelLifecycle(connection: Connection) extends VanillaLifecycle[Channel] with StrictLogging {
  override def start(): Channel =
    RabbitChannel(connection)



  override def shutdown(channel: Channel): Unit = {
    if (connection.isOpen) {
      channel.close()
    }
  }
}
