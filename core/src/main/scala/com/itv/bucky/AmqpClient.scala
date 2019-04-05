package com.itv.bucky

import cats.effect._
import cats.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery, PublishCommand}
import com.rabbitmq.client.{Channel => RabbitChannel, Connection => RabbitConnection}
import com.itv.bucky.decl._
import com.rabbitmq.client.{ConnectionFactory, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds

trait AmqpClient[F[_]] {
  def declare(declarations: Declaration*): F[Unit]
  def declare(declarations: Iterable[Declaration]): F[Unit]
  def publisher(): Publisher[F, PublishCommand]
  def registerConsumer(queueName: QueueName,
                       handler: Handler[F, Delivery],
                       exceptionalAction: ConsumeAction = DeadLetter): F[Unit]
  def isConnectionOpen: F[Boolean]
}

object AmqpClient extends StrictLogging {

  private def createChannel[F[_]](connection: RabbitConnection)(implicit F: Sync[F], cs: ContextShift[F]): Resource[F, RabbitChannel] = {
    val make =
      F.delay {
        logger.info(s"Starting Channel")
        val channel = connection.createChannel()
        channel.addShutdownListener((cause: ShutdownSignalException) => logger.warn(s"Channel shut down", cause))
        channel
      }
        .attempt
        .flatTap {
          case Right(_) =>
            F.delay(logger.info(s"Channel has been started successfully!"))
          case Left(exception) =>
            F.delay(logger.error(s"Failure when starting Channel because ${exception.getMessage}", exception))
        }
        .rethrow

    Resource.make(cs.shift.flatMap(_ => make))(channel => F.delay(channel.close()))
  }

  private def createConnection[F[_]](config: AmqpClientConfig)(implicit F: Sync[F], cs: ContextShift[F]): Resource[F, RabbitConnection] = {
    val make =
      F.delay {
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
      }
        .attempt
        .flatTap {
          case Right(_) =>
            logger.info(s"AmqpClient has been started successfully!").pure[F]
          case Left(exception) =>
            logger.error(s"Failure when starting AmqpClient because ${exception.getMessage}", exception).pure[F]
        }
        .rethrow

    Resource.make(cs.shift.flatMap(_ => make))(connection => F.delay(connection.close()))
  }

  def apply[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): Resource[F, AmqpClient[F]] =
    for {
      connection <- createConnection(config)
      rabbitChannel = createChannel(connection).map(Channel.apply[F])
      client <- apply[F](config, rabbitChannel)
    } yield client

  def apply[F[_]](config: AmqpClientConfig,
                  channel: Resource[F, Channel[F]])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): Resource[F, AmqpClient[F]] =
    channel.flatMap { channel =>
      val make =
        for {
          _ <- cs.shift
          connectionManager <- AmqpClientConnectionManager(config, channel)
        }
          yield mkClient(connectionManager)
      Resource.make(make)(_ => F.unit)
    }

  private def mkClient[F[_]](
      connectionManager: AmqpClientConnectionManager[F])(implicit F: Concurrent[F], cs: ContextShift[F], t: Timer[F]): AmqpClient[F] =
    new AmqpClient[F] {
      override def publisher(): Publisher[F, PublishCommand] = cmd => {
        for {
          _ <- cs.shift
          _ <- connectionManager.publish(cmd)
        } yield ()
      }
      override def registerConsumer(queueName: QueueName,
                                    handler: Handler[F, Delivery],
                                    exceptionalAction: ConsumeAction): F[Unit] =
        connectionManager.registerConsumer(queueName, handler, exceptionalAction)

      override def declare(declarations: Declaration*): F[Unit]          = connectionManager.declare(declarations)
      override def declare(declarations: Iterable[Declaration]): F[Unit] = connectionManager.declare(declarations)

      override def isConnectionOpen: F[Boolean] = connectionManager.channel.isConnectionOpen
    }
}
