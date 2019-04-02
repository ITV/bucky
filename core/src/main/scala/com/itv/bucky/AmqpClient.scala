package com.itv.bucky

import java.util.concurrent.TimeUnit

import cats.effect._
import cats.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery, PublishCommand}
import com.rabbitmq.client.{Channel => RabbitChannel, Connection => RabbitConnection}
import com.itv.bucky.decl._
import com.rabbitmq.client.{ConnectionFactory, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

trait AmqpClient[F[_]] {
  def declare(declarations: Declaration*): F[Unit]
  def declare(declarations: Iterable[Declaration]): F[Unit]
  def publisher(): Publisher[F, PublishCommand]
  def registerConsumer(queueName: QueueName,
                       handler: Handler[F, Delivery],
                       exceptionalAction: ConsumeAction = DeadLetter): F[Unit]
  def shutdown(): F[Unit]
}

object AmqpClient extends StrictLogging {

  private def createChannel[F[_]](connection: RabbitConnection)(implicit F: Sync[F]): F[RabbitChannel] =
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

  private def createConnection[F[_]](config: AmqpClientConfig)(implicit F: Sync[F]): F[RabbitConnection] =
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

  def apply[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClient[F]] =
    for {
      _          <- cs.shift
      connection <- createConnection(config)
      channel    <- createChannel(connection)
      client     <- apply(config, Channel(channel))
    } yield client

  def apply[F[_]](config: AmqpClientConfig,
                  channel: Channel[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClient[F]] =
    for {
      _                 <- cs.shift
      connectionManager <- AmqpClientConnectionManager(config, channel)
    } yield mkClient(connectionManager)

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

      override def shutdown(): F[Unit]                                   = connectionManager.shutdown()
      override def declare(declarations: Declaration*): F[Unit]          = connectionManager.declare(declarations)
      override def declare(declarations: Iterable[Declaration]): F[Unit] = connectionManager.declare(declarations)
    }
}
