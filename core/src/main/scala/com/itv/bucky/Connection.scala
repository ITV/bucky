package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging
import com.rabbitmq.client.{
  ConnectionFactory,
  ShutdownListener,
  ShutdownSignalException,
  Channel => RabbitChannel,
  Connection => RabbitConnection
}

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

object Connection extends StrictLogging {

  def apply(config: AmqpClientConfig): Either[Throwable, RabbitConnection] =
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
    }.toEither

  def close(connection: RabbitConnection): Unit =
    if (connection.isOpen) {
      logger.info(s"Closing connection: $connection")
      connection.close()
    }
}

object Channel extends StrictLogging {

  /**
    * Close channel and connection associated with the IdAmqpClient
    *
    * Note: This should not be used if you share the connection
    *
    * @param channel the channel
    */
  def closeAll(channel: RabbitChannel): Unit = {
    val connection = channel.getConnection
    Channel.close(channel)
    Connection.close(connection)
  }

  def close(channel: RabbitChannel): Unit =
    if (channel.getConnection.isOpen) {
      logger.info(s"Closing channel: $channel")
      channel.close()
    }

  def estimateMessageCount(channel: RabbitChannel, queueName: QueueName) =
    Try(Option(channel.basicGet(queueName.value, false)).fold(0)(_.getMessageCount + 1))

  def apply(connection: RabbitConnection): Either[Throwable, RabbitChannel] =
    Try {
      logger.info(s"Starting Channel")
      val channel = connection.createChannel()
      channel.addShutdownListener(new ShutdownListener() {
        override def shutdownCompleted(cause: ShutdownSignalException): Unit =
          logger.info(
            s"Channel shut down, cause reason: ${cause.getReason.protocolMethodName()}, " +
              s"is hard error: ${cause.isHardError}, is initiated by application: ${cause.isInitiatedByApplication}",
            cause.getStackTrace
          )
      })
      channel
    }.toEither
}


