package com.itv.bucky

import cats.effect.{ContextShift, IO, Timer}
import cats._
import cats.implicits._
import com.itv.bucky
import com.itv.bucky.decl.{Binding, Exchange, ExchangeBinding, Queue}
import com.rabbitmq.client.ConfirmListener
import com.itv.bucky.publish._
import com.itv.bucky.consume._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object SuperTest {
  class StubChannel extends Channel[IO] {
    var publishSeq: Long                                                         = 0L
    val exchanges: ListBuffer[Exchange]                                          = ListBuffer.empty
    val queues: ListBuffer[Queue]                                                = ListBuffer.empty
    val bindings: ListBuffer[Binding]                                            = ListBuffer.empty
    val exchangeBindings: ListBuffer[ExchangeBinding]                            = ListBuffer.empty
    val handlers: mutable.Map[QueueName, (Handler[IO, Delivery], ConsumeAction)] = mutable.Map.empty
    val confirmListeners: ListBuffer[ConfirmListener]                            = ListBuffer.empty

    override def close(): IO[Unit]                        = IO.unit
    override def shutdownChannelAndConnection(): IO[Unit] = IO.unit
    override def purgeQueue(name: QueueName): IO[Unit]    = IO.unit
    override def basicQos(prefetchCount: Int): IO[Unit]   = IO.unit
    override def confirmSelect: IO[Unit]                  = IO.unit
    override def getNextPublishSeqNo: IO[Long] = IO.delay {
      publishSeq
    }

    override def publish(cmd: PublishCommand): IO[Unit] = {
      val queues = bindings.find(binding => binding.exchangeName == cmd.exchange && binding.routingKey == cmd.routingKey).map(_.queueName)
      val subscribedHandlers = handlers
        .filterKeys(queues.contains)
        .mapValues {
          case (handler, _) => handler
        }
        .values
        .toList
      (for {
        delivery <- deliveryFor(cmd)
        _        <- subscribedHandlers.traverse(_(delivery))
        _        <- confirmListeners.toList.traverse(cl => IO.delay(cl.handleAck(delivery.envelope.deliveryTag, false)))
      } yield ()).attempt
        .flatTap(_ => IO.delay(publishSeq = publishSeq + 1))
        .rethrow
    }

    private def deliveryFor(publishCommand: PublishCommand): IO[Delivery] =
      for {
        publishSeq <- getNextPublishSeqNo
      } yield
        Delivery(
          publishCommand.body,
          ConsumerTag("test"),
          Envelope(publishSeq, redeliver = false, publishCommand.exchange, publishCommand.routingKey),
          publishCommand.basicProperties
        )

    override def sendAction(action: ConsumeAction)(envelope: bucky.Envelope): IO[Unit] =
      IO.unit

    override def registerConsumer(handler: Handler[IO, Delivery], onFailure: ConsumeAction, queue: QueueName, consumerTag: ConsumerTag): IO[Unit] =
      IO.delay(handlers.synchronized(handlers.put(queue, handler -> onFailure))).void

    override def addConfirmListener(listener: ConfirmListener): IO[Unit] = IO.delay(confirmListeners.synchronized(confirmListeners += listener))
    override def declareExchange(exchange: Exchange): IO[Unit]           = IO.delay(exchanges.synchronized(exchanges += exchange)).void
    override def declareQueue(queue: Queue): IO[Unit]                    = IO.delay(queues.synchronized(queues += queue)).void
    override def declareBinding(binding: Binding): IO[Unit] = {
      val hasExchange = exchanges.exists(_.name == binding.exchangeName)
      val hasQueue    = queues.exists(_.name == binding.queueName)
      if (hasExchange && hasQueue) {
        IO.delay(bindings.synchronized(bindings += binding))
      } else {
        IO.raiseError(new RuntimeException(s"Binding $binding had queue $hasQueue and exchange $hasExchange"))
      }
    }

    override def declareExchangeBinding(binding: ExchangeBinding): IO[Unit] = {
      val hasDestExchange   = exchanges.exists(_.name == binding.destinationExchangeName)
      val hasSourceExchange = exchanges.exists(_.name == binding.sourceExchangeName)

      if (hasDestExchange && hasSourceExchange) {
        IO.delay(exchangeBindings.synchronized(exchangeBindings += binding))
      } else {
        IO.raiseError(new RuntimeException(s"Exchange Binding $binding had destination $hasDestExchange and source $hasSourceExchange"))
      }
    }
  }

  object StubChannel {
    def working: StubChannel = new StubChannel
    def publishTimeout: StubChannel = new StubChannel {
      override def publish(cmd: PublishCommand): IO[Unit] = IO.unit
    }

  }

  def withDefaultClient(publishTimeout: FiniteDuration = 5.seconds, channel: Channel[IO] = StubChannel.working)(
      test: AmqpClient[IO] => IO[Unit]): Unit = {
    val ec                            = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)
    val config                        = AmqpClientConfig("127.0.0.1", 5672, "guest", "guest", publishingTimeout = publishTimeout)
    AmqpClient.apply[IO](config, channel).flatMap(test).unsafeRunSync()
  }
}
