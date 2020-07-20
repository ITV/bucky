package com.itv.bucky
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import cats.effect.{ConcurrentEffect, ContextShift, IO, Sync}
import cats.effect.implicits._
import cats.implicits._
import com.itv.bucky.consume.{Ack, ConsumeAction, Consumer, ConsumerTag, DeadLetter, Delivery, RequeueImmediately}
import com.itv.bucky.decl.{Binding, Declaration, Exchange, ExchangeBinding, Queue}
import com.itv.bucky.publish.PublishCommand
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection
import com.rabbitmq.client.{ConfirmListener, DefaultConsumer, Channel => RabbitChannel, Envelope => RabbitMQEnvelope}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait Channel[F[_]] {
  def isConnectionOpen: F[Boolean]
  def synchroniseIfNeeded[T](f: => T): T
  def close(): F[Unit]
  def purgeQueue(name: QueueName): F[Unit]
  def basicQos(prefetchCount: Int): F[Unit]
  def confirmSelect: F[Unit]
  def addConfirmListener(listener: ConfirmListener): F[Unit]
  def getNextPublishSeqNo: F[Long]
  def publish(sequenceNumber: Long, cmd: PublishCommand): F[Unit]
  def sendAction(action: ConsumeAction)(envelope: Envelope): F[Unit]
  def declareExchange(exchange: Exchange): F[Unit]
  def declareQueue(queue: Queue): F[Unit]
  def declareBinding(binding: Binding): F[Unit]
  def declareExchangeBinding(binding: ExchangeBinding): F[Unit]
  def runDeclarations(declaration: Iterable[Declaration])(implicit F: Sync[F]): F[Unit] = {
    val queues           = declaration.collect { case q: Queue            => q }.toList
    val exchanges        = declaration.collect { case e: Exchange         => e }.toList
    val exchangeBindings = declaration.collect { case eb: ExchangeBinding => eb }.toList
    val bindings = declaration
      .collect {
        case b: Binding  => List(b)
        case e: Exchange => e.bindings
      }
      .toList
      .flatten

    for {
      _ <- queues.traverse(declareQueue)
      _ <- exchanges.traverse(declareExchange)
      _ <- bindings.traverse(declareBinding)
      _ <- exchangeBindings.traverse(declareExchangeBinding)
    } yield ()
  }
  def registerConsumer(handler: Handler[F, Delivery],
                       onHandlerException: ConsumeAction,
                       queue: QueueName,
                       consumerTag: ConsumerTag,
                       cs: ContextShift[F]): F[Unit]
}

object Channel {
  def apply[F[_]](channel: RabbitChannel)(implicit F: ConcurrentEffect[F]): Channel[F] = new Channel[F] with StrictLogging {
    import scala.jdk.CollectionConverters._

    override def close(): F[Unit]                                       = F.delay(channel.close())
    override def purgeQueue(name: QueueName): F[Unit]                   = F.delay { channel.queuePurge(name.value) }
    override def basicQos(prefetchCount: Int): F[Unit]                  = F.delay(channel.basicQos(prefetchCount)).void
    override def confirmSelect: F[Unit]                                 = F.delay(channel.confirmSelect)
    override def addConfirmListener(listener: ConfirmListener): F[Unit] = F.delay(channel.addConfirmListener(listener))
    override def getNextPublishSeqNo: F[Long]                           = F.delay(channel.getNextPublishSeqNo)

    override def publish(sequenceNumber: Long, cmd: PublishCommand): F[Unit] =
      for {
        _ <- F.delay(logger.debug("Publishing command with exchange:{} rk: {}.", cmd.exchange, cmd.routingKey))
        _ <- F.delay(
          channel
            .basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, MessagePropertiesConverters(cmd.basicProperties), cmd.body.value)
        )
        _ <- F.delay(logger.info("Published message: {}", cmd))
      } yield ()

    override def sendAction(action: ConsumeAction)(envelope: Envelope): F[Unit] = action match {
      case Ack                => F.delay(channel.basicAck(envelope.deliveryTag, false))
      case DeadLetter         => F.delay(channel.basicNack(envelope.deliveryTag, false, false))
      case RequeueImmediately => F.delay(channel.basicNack(envelope.deliveryTag, false, true))
    }

    def declareExchange(exchange: Exchange): F[Unit] =
      F.delay {
        channel.exchangeDeclare(exchange.name.value,
                                exchange.exchangeType.value,
                                exchange.isDurable,
                                exchange.shouldAutoDelete,
                                exchange.isInternal,
                                exchange.arguments.asJava)
      }.void

    override def declareQueue(queue: Queue): F[Unit] =
      F.delay {
        channel.queueDeclare(queue.name.value, queue.isDurable, queue.isExclusive, queue.shouldAutoDelete, queue.arguments.asJava)
      }.void

    override def declareBinding(binding: Binding): F[Unit] =
      F.delay {
        channel.queueBind(binding.queueName.value, binding.exchangeName.value, binding.routingKey.value, binding.arguments.asJava)
      }.void

    override def declareExchangeBinding(binding: ExchangeBinding): F[Unit] =
      F.delay {
        channel
          .exchangeBind(
            binding.destinationExchangeName.value,
            binding.sourceExchangeName.value,
            binding.routingKey.value,
            binding.arguments.asJava
          )
      }.void

    override def registerConsumer(handler: Handler[F, Delivery],
                                  onHandlerException: ConsumeAction,
                                  queue: QueueName,
                                  consumerTag: ConsumerTag,
                                  cs: ContextShift[F]): F[Unit] = {

      val deliveryCallback = new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          val delivery = Consumer.deliveryFrom(consumerTag, envelope, properties, body)
          (for {
            _        <- cs.shift
            _        <- F.delay(logger.debug("Received delivery with rk:{} on exchange: {}", delivery.envelope.routingKey, delivery.envelope.exchangeName))
            action   <- handler(delivery)
            _        <- F.delay(logger.info("Responding with {} to {} on {}", action, delivery, queue))
          } yield action).attempt
            .flatTap {
              case Left(e) =>
                F.point {
                  logger.error(s"Handler exception whilst processing delivery: $delivery on $queue", e)
                }
              case Right(_) =>
                F.point {
                  logger.debug("Processed message with dl {}", envelope.getDeliveryTag)
                }
            }
            .flatMap {
              case Right(r) => F.delay(r)
              case Left(e)  => F.delay(logger.debug(s"Handler failure with {} will recover to: {}", e.getMessage, onHandlerException)) *> F.delay(onHandlerException)
            }
            .flatMap(sendAction(_)(Envelope.fromEnvelope(envelope)))
            .toIO
            .unsafeRunAsyncAndForget
          }
      }
      F.delay(channel.basicConsume(queue.value, false, consumerTag.value, deliveryCallback)).void
    }

    override def synchroniseIfNeeded[T](f: => T): T = this.synchronized(f)

    override def isConnectionOpen: F[Boolean] = F.delay(channel.getConnection.isOpen)
  }

}
