package com.itv.bucky

import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import cats.effect._
import cats.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery, PublishCommand}
import com.rabbitmq.client.{ConnectionFactory, ShutdownListener, ShutdownSignalException, Channel => RabbitChannel, Connection => RabbitConnection}
import com.itv.bucky.decl._
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.language.higherKinds

trait AmqpClient[F[_]] {
  def declare(declarations: Declaration*): F[Unit]
  def declare(declarations: Iterable[Declaration]): F[Unit]
  def publisher(): Publisher[F, PublishCommand]
  def registerConsumer(queueName: QueueName,
                       handler: Handler[F, Delivery],
                       exceptionalAction: ConsumeAction = DeadLetter): Resource[F, Unit]
  def isConnectionOpen: F[Boolean]
}

object AmqpClient extends StrictLogging {

  private def createChannel[F[_]](connection: RabbitConnection)(implicit F: Sync[F], cs: ContextShift[F]): Resource[F, RabbitChannel] = {
    val make =
      F.delay {
        logger.info(s"Starting Channel")
        val channel = connection.createChannel()
        channel.addShutdownListener(new ShutdownListener {
          override def shutdownCompleted(cause: ShutdownSignalException): Unit =
            if (cause.isInitiatedByApplication)
              logger.info(s"Channel shut down due to explicit application action: ${cause.getMessage}")
            else
              logger.error(s"Channel shut down by broker or because of detectable non-deliberate application failure", cause)
        })
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

  private def createConnection[F[_]](config: AmqpClientConfig)(implicit F: Sync[F], cs: ContextShift[F], executionContext: ExecutionContext): Resource[F, RabbitConnection] = {
    val make =
      F.delay {
        logger.info(s"Starting AmqpClient")
        val connectionFactory = new ConnectionFactory()
        connectionFactory.setHost(config.host)
        connectionFactory.setPort(config.port)
        connectionFactory.setUsername(config.username)
        connectionFactory.setPassword(config.password)
        connectionFactory.setAutomaticRecoveryEnabled(config.networkRecoveryInterval.isDefined)
        connectionFactory.setSharedExecutor(executionContext match {
          case null => throw null
          case eces: ExecutionContextExecutorService => eces
          case other => new AbstractExecutorService with ExecutionContextExecutorService {
            override def prepare(): ExecutionContext = other
            override def isShutdown = false
            override def isTerminated = false
            override def shutdown() = ()
            override def shutdownNow() = Collections.emptyList[Runnable]
            override def execute(runnable: Runnable): Unit = other execute runnable
            override def reportFailure(t: Throwable): Unit = other reportFailure t
            override def awaitTermination(length: Long,unit: TimeUnit): Boolean = false
          }
        })
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

  def apply[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F], executionContext: ExecutionContext): Resource[F, AmqpClient[F]] =
    for {
      connection <- createConnection(config)
      publishChannel = createChannel(connection).map(Channel.apply[F])
      buildChannel = () => createChannel(connection).map(Channel.apply[F])
      client <- apply[F](config, buildChannel, publishChannel)
    } yield client

  def apply[F[_]](config: AmqpClientConfig,
                  buildChannel: () => Resource[F, Channel[F]],
                  publishChannel: Resource[F, Channel[F]])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F], executionContext: ExecutionContext): Resource[F, AmqpClient[F]] =
    publishChannel.flatMap { channel =>
      val make =
        for {
          _ <- cs.shift
          connectionManager <- AmqpClientConnectionManager(config, channel)
        }
          yield mkClient(buildChannel, connectionManager)
      Resource.make(make)(_ => F.unit)
    }

  private def mkClient[F[_]](
      buildChannel: () => Resource[F, Channel[F]],
      connectionManager: AmqpClientConnectionManager[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): AmqpClient[F] =
    new AmqpClient[F] {
      override def publisher(): Publisher[F, PublishCommand] = cmd => {
        for {
          _ <- cs.shift
          _ <- connectionManager.publish(cmd)
        } yield ()
      }
      override def registerConsumer(queueName: QueueName,
                                    handler: Handler[F, Delivery],
                                    exceptionalAction: ConsumeAction): Resource[F, Unit] =
        buildChannel().evalMap {
          connectionManager.registerConsumer(_, queueName, handler, exceptionalAction)
        }

      override def declare(declarations: Declaration*): F[Unit]          = connectionManager.declare(declarations)
      override def declare(declarations: Iterable[Declaration]): F[Unit] = connectionManager.declare(declarations)

      override def isConnectionOpen: F[Boolean] = connectionManager.publishChannel.isConnectionOpen
    }
}
