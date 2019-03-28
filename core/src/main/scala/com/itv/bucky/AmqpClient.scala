package com.itv.bucky

import java.util.concurrent.TimeUnit

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import cats.effect.implicits._
import com.rabbitmq.client.{
  ConfirmCallback,
  ConfirmListener,
  ConnectionFactory,
  DefaultConsumer,
  ReturnListener,
  ShutdownSignalException,
  Channel => RabbitChannel,
  Connection => RabbitConnection,
  Envelope => RabbitMQEnvelope
}
import com.itv.bucky.decl.{Binding, Exchange, ExchangeBinding, Queue}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.language.higherKinds

trait AmqpClient[F[_]] {
  def performOps(thunk: AmqpOps => F[Unit]): F[Unit]
  def estimatedMessageCount(queueName: QueueName): F[Long]
  def publisher(timeout: FiniteDuration = FiniteDuration(10, TimeUnit.SECONDS)): Publisher[F, PublishCommand]
  def consumer(queueName: QueueName,
               handler: Handler[F, Delivery],
               exceptionalAction: ConsumeAction = DeadLetter,
               prefetchCount: Int = 0): F[Unit]
}

case class AmqpClientConnectionManager[F[_]](amqpConfig: AmqpClientConfig)(implicit F: ConcurrentEffect[F],
                                                                           cs: ContextShift[F],
                                                                           t: Timer[F])
    extends StrictLogging {

  def createChannel(connection: RabbitConnection): F[RabbitChannel] =
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

  /*

          _ <- F.delay(channel.confirmSelect())
          _ <- F.delay(
            channel.addConfirmListener(
              (dt: Long, multiple: Boolean) => unconfirmedPublications.get.map(_(dt)(Right(()))).toIO.unsafeRunSync(),
              (dt: Long, multiple: Boolean) => logger.error(s"Nack Received $dt $multiple")
            )
          )
   */

  def createConnection(config: AmqpClientConfig): F[RabbitConnection] =
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

object AmqpClient extends StrictLogging {

  private def confirmListener[F[_]](pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]])(
      implicit F: Sync[F]): ConfirmListener =
    new ConfirmListener {
      private def pop[T](deliveryTag: Long, multiple: Boolean): F[List[Deferred[F, Boolean]]] =
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
        pop(deliveryTag, multiple).flatMap { toComplete =>
          logger.error("Received ack for delivery tag: {} and multiple: {}", deliveryTag, multiple)
          toComplete.map(_.complete(true)).sequence
        }

      override def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
        pop(deliveryTag, multiple).flatMap { toComplete =>
          logger.error("Received Nack for delivery tag: {} and multiple: {}", deliveryTag, multiple)
          toComplete.map(_.complete(false)).sequence
        }
    }

  def apply[F[_]](
      config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClient[F]] =
    for {
      _                    <- cs.shift
      connectionManager    <- F.delay(AmqpClientConnectionManager(config))
      connection           <- connectionManager.createConnection(config)
      channel              <- connectionManager.createChannel(connection)
      _                    <- F.delay(channel.confirmSelect())
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Boolean]]](TreeMap.empty)
      _                    <- F.delay(channel.addConfirmListener(confirmListener(pendingConfirmations)))
    } yield mkClient(channel, pendingConfirmations)

  private def mkClient[F[_]](channel: RabbitChannel, pendingConfirmations: Ref[F, TreeMap[Long, Deferred[F, Boolean]]])(
      implicit F: ConcurrentEffect[F],
      cs: ContextShift[F],
      t: Timer[F]): AmqpClient[F] =
    new AmqpClient[F] {

      override def performOps(thunk: AmqpOps => F[Unit]): F[Unit]       = thunk(ChannelAmqpOps(channel))
      override def estimatedMessageCount(queueName: QueueName): F[Long] = F.delay(channel.messageCount(queueName.value))
      override def publisher(timeout: FiniteDuration): Publisher[F, PublishCommand] = cmd => {
        (for {
          _      <- cs.shift
          signal <- Deferred[F, Boolean]
          _ <- channel.synchronized {
            val deliveryTag = channel.getNextPublishSeqNo

            pendingConfirmations
              .update(_ + (deliveryTag -> signal))
              .map { _ =>
                channel.basicPublish(cmd.exchange.value,
                                     cmd.routingKey.value,
                                     false,
                                     false,
                                     MessagePropertiesConverters(cmd.basicProperties),
                                     cmd.body.value)
              }
          }
          isAck <- signal.get
          _     <- if (isAck) F.pure(()) else F.raiseError[Unit](new RuntimeException("Publish failed"))
        } yield ()).timeout(timeout)
      }

      import cats.effect.implicits._

      override def consumer(queueName: QueueName,
                            handler: Handler[F, Delivery],
                            exceptionalAction: ConsumeAction,
                            prefetchCount: Int): F[Unit] = {
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

trait AmqpOps {
  def declareQueue(queue: Queue): Try[Unit]
  def declareExchange(exchange: Exchange): Try[Unit]
  def bindQueue(binding: Binding): Try[Unit]
  def bindExchange(binding: ExchangeBinding): Try[Unit]
  def purgeQueue(name: QueueName): Try[Unit]
}

//object AmqpClient extends StrictLogging {
//
//  def publisherOf[F[_], T](commandBuilder: PublishCommandBuilder[T])(publisher: Publisher[F, PublishCommand])(
//      implicit F: Monad[F]): Publisher[F, T] =
//    (message: T) =>
//      F.flatMap(F.apply {
//        commandBuilder.toPublishCommand(message)
//      }) { publisher }
//
//  def deliveryHandlerOf[F[_], T](
//      handler: Handler[F, T],
//      unmarshaller: DeliveryUnmarshaller[T],
//      unmarshalFailureAction: ConsumeAction = DeadLetter)(implicit monad: Monad[F]): Handler[F, Delivery] =
//    new DeliveryUnmarshalHandler[F, T, ConsumeAction](unmarshaller)(handler, unmarshalFailureAction)
//
//  def handlerOf[F[_], T](
//      handler: Handler[F, T],
//      unmarshaller: PayloadUnmarshaller[T],
//      unmarshalFailureAction: ConsumeAction = DeadLetter)(implicit monad: Monad[F]): Handler[F, Delivery] =
//    deliveryHandlerOf(handler, Unmarshaller.toDeliveryUnmarshaller(unmarshaller), unmarshalFailureAction)
//
//}
