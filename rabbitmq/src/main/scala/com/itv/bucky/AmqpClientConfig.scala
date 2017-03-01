package com.itv.bucky

import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class AmqpClientLifecycle(config: AmqpClientConfig) extends Lifecycle[AmqpClient] with StrictLogging {

  override type ServiceInstance = Connection

  override def start(): Connection = {
    Try {
      logger.info(s"Starting AmqpClient")
      val connectionFactory = new ConnectionFactory()
      connectionFactory.setHost(config.host)
      connectionFactory.setPort(config.port)
      connectionFactory.setUsername(config.username)
      connectionFactory.setPassword(config.password)
      connectionFactory.setAutomaticRecoveryEnabled(config.networkRecoveryInterval.isDefined)
      config.networkRecoveryInterval.map(_.toMillis.toInt).foreach(connectionFactory.setNetworkRecoveryInterval)
      config.virtualHost.foreach(connectionFactory.setVirtualHost)
      connectionFactory.newConnection()
    } match {
      case Success(connection) =>
        logger.info(s"AmqpClient has been started successfully!")
        connection
      case Failure(exception) =>
        logger.error(s"Failure when starting AmqpClient because ${exception.getMessage}", exception)
        throw exception
    }
  }

  override def unwrap(instance: Connection): AmqpClient = new RawAmqpClient(AmqpChannelLifecycle(instance))

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
    Try {
      logger.info(s"Starting Channel")
      connection.createChannel()
    } match {
      case Success(channel) =>
        logger.info(s"Channel has been started successfully!")
        channel
      case Failure(exception) =>
        logger.error(s"Failure when starting Channel because ${exception.getMessage}", exception)
        throw exception
    }

  override def shutdown(channel: Channel): Unit = {
    if (connection.isOpen) {
      channel.close()
    }
  }
}
