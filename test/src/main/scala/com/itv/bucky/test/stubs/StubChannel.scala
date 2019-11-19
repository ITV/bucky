package com.itv.bucky.test.stubs

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.itv.bucky
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.{Channel, Envelope, Handler, QueueName}
import com.itv.bucky.decl.{Binding, Exchange, ExchangeBinding, Queue}
import com.rabbitmq.client.ConfirmListener
import cats._
import cats.effect._
import cats.effect.ConcurrentEffect
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

abstract class StubChannel[F[_]](implicit F: ConcurrentEffect[F]) extends Channel[F] with StrictLogging {
  var publishSeq: Long                                                        = 0L
  val pubSeqLock: Object                                                      = new Object
  val exchanges: ListBuffer[Exchange]                                         = ListBuffer.empty
  val queues: ListBuffer[Queue]                                               = ListBuffer.empty
  val bindings: ListBuffer[Binding]                                           = ListBuffer.empty
  val exchangeBindings: ListBuffer[ExchangeBinding]                           = ListBuffer.empty
  val handlers: mutable.Map[QueueName, (Handler[F, Delivery], ConsumeAction)] = mutable.Map.empty
  val confirmListeners: ListBuffer[ConfirmListener]                           = ListBuffer.empty

  override def close(): F[Unit]                      = F.unit
  override def purgeQueue(name: QueueName): F[Unit]  = F.unit
  override def basicQos(prefetchCount: Int): F[Unit] = F.unit
  override def confirmSelect: F[Unit]                = F.unit

  override def getNextPublishSeqNo: F[Long] =
    F.delay(pubSeqLock.synchronized {
      val current = publishSeq
      publishSeq = publishSeq + 1
      current
    })

  def handlePublishHandlersResult(result: Either[Throwable, List[ConsumeAction]]): F[Unit]

  private def lookupQueues(publishCommand: PublishCommand): List[QueueName] = {
    val exchangeBindingExchanges =
      exchangeBindings
        .filter(b => b.sourceExchangeName == publishCommand.exchange && b.routingKey == publishCommand.routingKey)
        .map(_.destinationExchangeName)
        .toSet

    ((bindings
      .filter(binding => binding.exchangeName == publishCommand.exchange && binding.routingKey == publishCommand.routingKey)
      .map(_.queueName)
      .toList)
    ++
    (bindings
      .filter(binding => exchangeBindingExchanges(binding.exchangeName) && binding.routingKey == publishCommand.routingKey)
      .map(_.queueName)
      .toList))
  }

  override def publish(sequenceNumber: Long, cmd: PublishCommand): F[Unit] = {
    val queues = lookupQueues(cmd)
    val subscribedHandlers = handlers
      .filterKeys(queues.contains)
      .mapValues {
        case (handler, _) => handler
      }
      .values
      .toList
    (for {
      id       <- F.delay(UUID.randomUUID())
      _        <- F.delay(logger.debug("Publishing message with rk: {}, exchange: {} ,body: {} and pid {}", cmd.routingKey, cmd.exchange, cmd.body, id))
      _        <- F.delay(logger.debug("Found {} queues for pid {}.", queues.toList.map(_.value), id))
      _        <- F.delay(logger.debug("Found {} handlers for pid {}.", subscribedHandlers.size, id))
      delivery <- deliveryFor(cmd, sequenceNumber)
      result   <- subscribedHandlers.traverse(_(delivery)).attempt
      _        <- F.delay(logger.debug("Message pid {} published with result {}.", id, result))
      _        <- handlePublishHandlersResult(result)
      _        <- confirmListeners.toList.traverse(cl => F.delay(cl.handleAck(delivery.envelope.deliveryTag, false)))
    } yield ()).attempt.rethrow
  }

  private def deliveryFor(publishCommand: PublishCommand, sequenceNumber: Long): F[Delivery] =
    F.pure(
      Delivery(
        publishCommand.body,
        ConsumerTag("test"),
        Envelope(sequenceNumber, redeliver = false, publishCommand.exchange, publishCommand.routingKey),
        publishCommand.basicProperties
      )
    )

  override def sendAction(action: ConsumeAction)(envelope: bucky.Envelope): F[Unit] =
    F.unit

  override def registerConsumer(handler: Handler[F, Delivery],
                                onHandlerException: ConsumeAction,
                                queue: QueueName,
                                consumerTag: ConsumerTag,
                                cs: ContextShift[F]): F[Unit] =
    F.delay(handlers.synchronized(handlers.put(queue, handler -> onHandlerException))).void

  override def addConfirmListener(listener: ConfirmListener): F[Unit] = F.delay(confirmListeners.synchronized(confirmListeners += listener))
  override def declareExchange(exchange: Exchange): F[Unit]           = F.delay(exchanges.synchronized(exchanges += exchange)).void
  override def declareQueue(queue: Queue): F[Unit]                    = F.delay(queues.synchronized(queues += queue)).void
  override def declareBinding(binding: Binding): F[Unit] = {
    val hasExchange = exchanges.exists(_.name == binding.exchangeName)
    val hasQueue    = queues.exists(_.name == binding.queueName)
    if (hasExchange && hasQueue) {
      F.delay(bindings.synchronized(bindings += binding))
    } else {
      F.raiseError(new RuntimeException(s"Binding $binding had queue $hasQueue and exchange $hasExchange"))
    }
  }

  override def declareExchangeBinding(binding: ExchangeBinding): F[Unit] = {
    val hasDestExchange   = exchanges.exists(_.name == binding.destinationExchangeName)
    val hasSourceExchange = exchanges.exists(_.name == binding.sourceExchangeName)

    if (hasDestExchange && hasSourceExchange) {
      F.delay(exchangeBindings.synchronized(exchangeBindings += binding))
    } else {
      F.raiseError(
        new RuntimeException(
          s"Error finding source exchange (result=$hasSourceExchange) and destination exchange(result=$hasDestExchange) for binding $binding.")
      )
    }
  }

  override def synchroniseIfNeeded[T](f: => T): T = f

  override def isConnectionOpen: F[Boolean] = F.pure(true)
}
