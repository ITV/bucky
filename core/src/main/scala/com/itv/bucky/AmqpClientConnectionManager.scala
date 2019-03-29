package com.itv.bucky

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import com.itv.bucky.decl.Declaration
import com.rabbitmq.client.{
  ConfirmListener,
  ConnectionFactory,
  DefaultConsumer,
  ShutdownSignalException,
  Channel => RabbitChannel,
  Connection => RabbitConnection,
  Envelope => RabbitMQEnvelope
}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try

private[bucky] case class PendingConfirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]])(
    implicit sync: ConcurrentEffect[F])
    extends ConfirmListener
    with StrictLogging {

  def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, Boolean]]] =
    pendingConfirmations.modify { x =>
      if (multiple) {
        val entries     = x.until(deliveryTag + 1).toList
        val nextPending = x -- entries.map { case (key, _) => key }

        (nextPending, entries.map { case (_, value) => value })
      } else {
        val nextPending = x - deliveryTag

        (nextPending, x.get(deliveryTag).toList)
      }
    }

  override def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
    pop(deliveryTag, multiple)
      .flatMap { toComplete =>
        logger.error("Received ack for delivery tag: {} and multiple: {}", deliveryTag, multiple)
        toComplete.map(_.complete(true)).sequence
      }
      .toIO
      .unsafeRunSync()

  override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
    pop(deliveryTag, multiple)
      .flatMap { toComplete =>
        logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple)
        toComplete.map(_.complete(false)).sequence
      }
      .toIO
      .unsafeRunSync()
}

private[bucky] case class AmqpClientConnectionManager[F[_]](
    amqpConfig: AmqpClientConfig,
    channel: RabbitChannel,
    pendingConfirmListener: PendingConfirmListener[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F])
    extends StrictLogging {

  private def runWithChannelSync[T](action: F[T]): F[T] =
    channel.synchronized {
      F.fromTry(Try {
        action.toIO.unsafeRunSync()
      })
    }

  def estimatedMessageCount(queueName: QueueName): F[Long] = F.delay(channel.messageCount(queueName.value))

  private[bucky] def internalPublish(
      timeout: FiniteDuration,
      cmd: PublishCommand,
      pendingConfListener: PendingConfirmListener[F],
      generateNextDeliveryTag: () => F[Long],
      publish: (PublishCommand, Deferred[F, Boolean], Long) => F[Unit]
  ) =
    for {
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, Boolean]
        _ <- runWithChannelSync {
          for {
            nextPublishSeq <- generateNextDeliveryTag()
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- publish(cmd, signal, nextPublishSeq)
          } yield ()
        }
        _ <- signal.get.ifM(F.unit, F.raiseError[Unit](new RuntimeException("Failed to publish msg.")))
      } yield ())
        .timeout(timeout)
        .recoverWith {
          case e =>
            runWithChannelSync {
              for {
                dl          <- deliveryTag.get
                deliveryTag <- F.fromOption(dl, new RuntimeException("Timeout occurred before a delivery tag could be obtained.", e))
                _           <- pendingConfListener.pop(deliveryTag, multiple = false)
                _           <- F.raiseError[Unit](e)
              } yield ()
            }
        }
    } yield ()

  def publish(timeout: FiniteDuration, cmd: PublishCommand): F[Unit] =
    internalPublish(
      timeout,
      cmd,
      pendingConfirmListener,
      () => F.delay(channel.getNextPublishSeqNo),
      publishMessage
    )

  private def publishMessage(cmd: PublishCommand, signal: Deferred[F, Boolean], deliveryTag: Long): F[Unit] =
    F.delay {
      channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, MessagePropertiesConverters(cmd.basicProperties), cmd.body.value)
    }

  def registerConsumer(queueName: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction, prefetchCount: Int): F[Unit] = {
    val consumeHandler = new DefaultConsumer(channel) {
      logger.info(s"Creating consumer for $queueName")
      override def handleDelivery(consumerTag: String,
                                  envelope: RabbitMQEnvelope,
                                  properties: com.rabbitmq.client.AMQP.BasicProperties,
                                  body: Array[Byte]): Unit = {
        val delivery = Consumer.deliveryFrom(consumerTag, envelope, properties, body)
        Consumer
          .processDelivery(channel, queueName, handler, exceptionalAction, delivery)
          .attempt
          .recoverWith {
            case throwable =>
              F.point {
                logger.error("Unhandled exception whilst processing delivery", throwable)
                Left(throwable)
              }
          }
          .void
          .toIO
          .unsafeRunSync
      }
    }

    for {
      consumerTag <- F.delay(ConsumerTag.create(queueName))
      _           <- cs.shift
      _           <- F.delay(channel.basicQos(prefetchCount))
      _           <- F.delay(channel.basicConsume(queueName.value, false, consumerTag.value, consumeHandler))
    } yield ()
  }
  def shutdown(): F[Unit] = F.delay(channel.close()).attempt.flatMap(_ => F.delay(channel.getConnection.close()))

  def declare(declarations: Iterable[Declaration]): F[Unit] =
    Declaration.runAll[F](declarations).apply(ChannelAmqpOps(channel))

}

private[bucky] object AmqpClientConnectionManager extends StrictLogging {

  def apply[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClientConnectionManager[F]] =
    for {
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Boolean]]](TreeMap.empty)
      connection           <- createConnection(config)
      channel              <- createChannel(connection)
      _                    <- F.delay(channel.confirmSelect())
      confirmListener      <- F.delay(PendingConfirmListener(pendingConfirmations))
      _                    <- F.delay(channel.addConfirmListener(confirmListener))
    } yield AmqpClientConnectionManager(config, channel, confirmListener)

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
}
