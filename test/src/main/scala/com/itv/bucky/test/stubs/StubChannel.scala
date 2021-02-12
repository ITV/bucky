package com.itv.bucky.test.stubs

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import com.itv.bucky
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.{Channel, Envelope, ExchangeName, Handler, QueueName, RoutingKey}
import com.itv.bucky.decl.{Binding, Direct, Exchange, ExchangeBinding, ExchangeType, Headers, Queue, Topic}
import com.rabbitmq.client.ConfirmListener
import cats._
import cats.effect._
import cats.effect.ConcurrentEffect
import cats.implicits._

import scala.language.higherKinds
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.itv.bucky.decl.Fanout
import org.typelevel.log4cats.Logger

import scala.collection.compat._

abstract class StubChannel[F[_]](implicit F: ConcurrentEffect[F], logger: Logger[F]) extends Channel[F] {
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
    val matchingExchanges = exchanges.filter(ex => ex.name == publishCommand.exchange)

    matchingExchanges.flatMap { ex =>
      if (ex.exchangeType == Headers) lookupQueuesForHeadersExchange(publishCommand)
      else if (ex.exchangeType == Fanout) lookupQueuesForFanoutExchange(publishCommand)
      else lookupQueuesForRoutingKeyExchange(publishCommand)
    }.toList
  }

  private def lookupExchangeType(exchangeName: ExchangeName): Option[ExchangeType] =
    exchanges.find(_.name == exchangeName).map(ex => ex.exchangeType)

  private def routingKeysMatch(decRk: RoutingKey, publishedRk: RoutingKey, exchangeType: Option[ExchangeType]): Boolean =
    exchangeType match {
      case Some(Topic) if decRk == publishedRk || decRk == RoutingKey("#") => true
      case Some(Topic) if decRk.value.count(_ == '#') == 1 && decRk.value.endsWith(".#") =>
        publishedRk.value.startsWith(decRk.value.replace(".#", ""))
      case Some(Topic) if decRk.value.contains("#")                        => throw new RuntimeException("Partial routing key matching not supported by bucky-test")
      case Some(_)                                                         => decRk == publishedRk
      case None                                                            => false
    }

  private def lookupQueuesForRoutingKeyExchange(publishCommand: PublishCommand): List[QueueName] = {
    val boundExchangesRkRouting =
      exchangeBindings
        .filter(
          b =>
            b.sourceExchangeName == publishCommand.exchange && routingKeysMatch(b.routingKey,
                                                                                publishCommand.routingKey,
                                                                                lookupExchangeType(b.sourceExchangeName)) && !lookupExchangeType(
              b.destinationExchangeName).contains(Headers))
        .map(_.destinationExchangeName)
        .toSet

    val boundExchangesHeaderRouting =
      exchangeBindings
        .filter(
          b =>
            b.sourceExchangeName == publishCommand.exchange && routingKeysMatch(b.routingKey,
                                                                                publishCommand.routingKey,
                                                                                lookupExchangeType(b.sourceExchangeName)) && lookupExchangeType(
              b.destinationExchangeName).contains(Headers))
        .map(_.destinationExchangeName)
        .toSet

    ((bindings
      .filter(
        binding =>
          binding.exchangeName == publishCommand.exchange && routingKeysMatch(binding.routingKey,
                                                                              publishCommand.routingKey,
                                                                              lookupExchangeType(binding.exchangeName)))
      .map(_.queueName)
      .toList)
      ++
        (bindings
          .filter(
            binding =>
              boundExchangesRkRouting(binding.exchangeName) && routingKeysMatch(binding.routingKey,
                                                                                publishCommand.routingKey,
                                                                                lookupExchangeType(binding.exchangeName)))
          .map(_.queueName)
          .toList)

      ++ boundExchangesHeaderRouting.flatMap(ex => lookupQueuesForHeadersExchange(publishCommand.copy(exchange = ex))))
  }

  private def lookupQueuesForHeadersExchange(publishCommand: PublishCommand): List[QueueName] = {
    def headersMatch(bindingArgs: Map[String, AnyRef]): Boolean = {
      val matchType = bindingArgs.getOrElse("x-match", "all")

      if (matchType == "any") {
        bindingArgs.view.filterKeys(_ != "x-match").exists(arg => publishCommand.basicProperties.headers.toSet.contains(arg))
      } else if (matchType == "all") {
        bindingArgs.view.filterKeys(_ != "x-match").forall(arg => publishCommand.basicProperties.headers.toSet.contains(arg))
      } else throw new RuntimeException(s"Binding declared with x-match argument not equal to 'any' or 'all'. Exchange: ${publishCommand.exchange}")
    }

    (bindings
      .filter(binding => binding.exchangeName == publishCommand.exchange && headersMatch(binding.arguments))
      .map(_.queueName)
      .toList)
  }

  private def lookupQueuesForFanoutExchange(publishCommand: PublishCommand): List[QueueName] = {
    (bindings
      .filter(binding => binding.exchangeName == publishCommand.exchange)
      .map(_.queueName)
      .toList)
  }

  override def publish(sequenceNumber: Long, cmd: PublishCommand): F[Unit] = {
    val queues = lookupQueues(cmd)
    val subscribedHandlers = handlers
      .view
      .filterKeys(queues.contains)
      .mapValues {
        case (handler, _) => handler
      }
      .toMap
      .values
      .toList
    (for {
      id       <- F.delay(UUID.randomUUID())
      _        <- logger.debug(s"Publishing message with rk: ${cmd.routingKey}, exchange: ${cmd.exchange} ,body: ${cmd.body} and pid $id")
      _        <- logger.debug(s"Found ${queues.map(_.value)} queues for pid $id.")
      _        <- logger.debug(s"Found ${subscribedHandlers.size} handlers for pid $id.")
      delivery = deliveryFor(cmd, sequenceNumber)
      result   <- subscribedHandlers.traverse(_(delivery)).attempt
      _        <- logger.debug(s"Message pid $id published with result $result.")
      _        <- handlePublishHandlersResult(result)
      _        <- confirmListeners.toList.traverse(cl => F.delay(cl.handleAck(delivery.envelope.deliveryTag, false)))
    } yield ()).attempt.rethrow
  }

  private def deliveryFor(publishCommand: PublishCommand, sequenceNumber: Long): Delivery =
    Delivery(
      publishCommand.body,
      ConsumerTag("test"),
      Envelope(sequenceNumber, redeliver = false, publishCommand.exchange, publishCommand.routingKey),
      publishCommand.basicProperties
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
