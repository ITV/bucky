package com.itv.bucky

import com.itv.bucky.Monad.Id
import com.itv.bucky.decl.{Binding, Exchange, Queue}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}
import com.rabbitmq.client.{Connection => RabbitConnection, Channel => RabbitChannel,_}


object Connection extends StrictLogging {
  def apply(config: AmqpClientConfig): Id[RabbitConnection] =
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


  def close(connection: RabbitConnection): Unit =
    if (connection.isOpen) {
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
    logger.info(s"Closing channel and conection for $channel")
    val connection = channel.getConnection
    Channel.close(channel)
    Connection.close(connection)
  }

  def close(channel: RabbitChannel): Unit =
    if (channel.getConnection.isOpen) {
      channel.close()
    }

  def estimateMessageCount(channel: RabbitChannel, queueName: QueueName) =
    Try(Option(channel.basicGet(queueName.value, false)).fold(0)(_.getMessageCount + 1))


  def apply(connection: RabbitConnection): Id[RabbitChannel] =
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


}

case class ChannelAmqpOps(channel: RabbitChannel) extends AmqpOps {

  import scala.collection.JavaConverters._

  override def declareExchange(exchange: Exchange): Try[Unit] = Try {
    channel.exchangeDeclare(
      exchange.name.value,
      exchange.exchangeType.value,
      exchange.isDurable,
      exchange.shouldAutoDelete,
      exchange.isInternal,
      exchange.arguments.asJava)
  }

  override def bindQueue(binding: Binding): Try[Unit] = Try {
    channel.queueBind(
      binding.queueName.value,
      binding.exchangeName.value,
      binding.routingKey.value,
      binding.arguments.asJava)
  }

  override def declareQueue(queue: Queue): Try[Unit] = Try {
    channel.queueDeclare(
      queue.name.value,
      queue.isDurable,
      queue.isExclusive,
      queue.shouldAutoDelete,
      queue.arguments.asJava)
  }

  override def purgeQueue(name: QueueName): Try[Unit] = Try {
    channel.queuePurge(name.value)
  }
}
