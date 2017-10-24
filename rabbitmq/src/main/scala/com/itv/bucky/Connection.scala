package com.itv.bucky

import com.itv.bucky.Monad.Id
import com.itv.bucky.decl.{Binding, Exchange, ExchangeBinding, Queue}
import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
import com.rabbitmq.client.{Channel => RabbitChannel, Connection => RabbitConnection, _}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

object Connection extends StrictLogging {

  def lifecycle(connection: => RabbitConnection): Lifecycle[RabbitConnection] = new VanillaLifecycle[RabbitConnection] {
    override def start(): RabbitConnection = connection

    override def shutdown(instance: RabbitConnection): Unit = Connection.close(instance)
  }

  def lifecycle(config: AmqpClientConfig): Lifecycle[RabbitConnection] = lifecycle(Connection(config))

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

  def lifecycle(connection: RabbitConnection): Lifecycle[RabbitChannel] = new VanillaLifecycle[RabbitChannel] {
    override def start(): RabbitChannel = Channel(connection)

    override def shutdown(instance: RabbitChannel): Unit = Channel.close(instance)
  }
}

case class ChannelAmqpOps(channel: RabbitChannel) extends AmqpOps {

  import scala.collection.JavaConverters._

  override def declareExchange(exchange: Exchange): Try[Unit] = Try {
    channel.exchangeDeclare(exchange.name.value,
                            exchange.exchangeType.value,
                            exchange.isDurable,
                            exchange.shouldAutoDelete,
                            exchange.isInternal,
                            exchange.arguments.asJava)
  }

  override def bindQueue(binding: Binding): Try[Unit] = Try {
    channel.queueBind(binding.queueName.value,
                      binding.exchangeName.value,
                      binding.routingKey.value,
                      binding.arguments.asJava)
  }

  override def bindExchange(binding: ExchangeBinding): Try[Unit] = Try {
    channel.exchangeBind(
      binding.destinationExchangeName.value,
      binding.sourceExchangeName.value,
      binding.routingKey.value,
      binding.arguments.asJava
    )
  }

  override def declareQueue(queue: Queue): Try[Unit] = Try {
    channel.queueDeclare(queue.name.value,
                         queue.isDurable,
                         queue.isExclusive,
                         queue.shouldAutoDelete,
                         queue.arguments.asJava)
  }

  override def purgeQueue(name: QueueName): Try[Unit] = Try {
    channel.queuePurge(name.value)
  }
}
