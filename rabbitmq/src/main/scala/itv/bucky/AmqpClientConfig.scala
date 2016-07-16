package itv.bucky

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import itv.contentdelivery.lifecycle.{Lifecycle, VanillaLifecycle}

import scala.concurrent.duration._

/**
  * AmqpClient configuration. Acts as a lifecycle factory for AmqpClient.
  */
case class AmqpClientConfig(host: String, port: Int, username: String, password: String, networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds)) extends Lifecycle[AmqpClient] {

  override type ServiceInstance = Connection

  override def unwrap(connection: Connection): AmqpClient = new RawAmqpClient(AmqpChannelLifecycle(connection))

  override def start(): Connection = {
    val connectionFactory = new ConnectionFactory()
    connectionFactory.setHost(host)
    connectionFactory.setPort(port)
    connectionFactory.setUsername(username)
    connectionFactory.setPassword(password)
    connectionFactory.setAutomaticRecoveryEnabled(networkRecoveryInterval.isDefined)
    networkRecoveryInterval.map(_.toMillis.toInt).foreach(connectionFactory.setNetworkRecoveryInterval)
    connectionFactory.newConnection()
  }

  override def shutdown(connection: Connection): Unit = connection.close()
}

case class AmqpChannelLifecycle(connection: Connection) extends VanillaLifecycle[Channel] {
  override def start(): Channel = connection.createChannel()

  override def shutdown(channel: Channel): Unit = channel.close()
}
