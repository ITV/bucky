package itv.bucky

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.{Lifecycle, VanillaLifecycle}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * AmqpClient configuration. Acts as a lifecycle factory for AmqpClient.
  */
case class AmqpClientConfig(
                             host: String,
                             port: Int,
                             username: String,
                             password: String,
                             networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds))
  extends Lifecycle[AmqpClient] with StrictLogging {

  override type ServiceInstance = Connection

  override def unwrap(connection: Connection): AmqpClient = new RawAmqpClient(AmqpChannelLifecycle(connection))

  override def start(): Connection = {
    Try {
      logger.info(s"Starting AmqpClient")
      val connectionFactory = new ConnectionFactory()
      connectionFactory.setHost(host)
      connectionFactory.setPort(port)
      connectionFactory.setUsername(username)
      connectionFactory.setPassword(password)
      connectionFactory.setAutomaticRecoveryEnabled(networkRecoveryInterval.isDefined)
      networkRecoveryInterval.map(_.toMillis.toInt).foreach(connectionFactory.setNetworkRecoveryInterval)
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

  override def shutdown(connection: Connection): Unit = {
    if (connection.isOpen) {
      connection.close()
    }
  }
}

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
