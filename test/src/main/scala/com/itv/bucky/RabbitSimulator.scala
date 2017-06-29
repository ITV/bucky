package com.itv.bucky

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.StrictLogging
import PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.decl.{Binding, Exchange, ExchangeBinding, Queue}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.language.higherKinds

case object IdentityBindings extends Bindings {
  def apply(routingQueue: RoutingKey): QueueName = QueueName(routingQueue.value)

  override def isDefinedAt(key: RoutingKey): Boolean = true
}


/**
  * Provides an AmqpClient implementation that simulates RabbitMQ server with one main difference:
  * Messages are sent directly to the consumer when published, there is no intermediate queue.
  * This makes it easy for tests to publish a message and see the corresponding ConsumeAction, e.g. Ack, Nack or Requeue.
  * Tests can use `RabbitSimulator.watchQueue` to see what messages get published to a queue that the application doesn't consume from.
  *
  * @param bindings A mapping from routing key to queue name, defaults to identity.
  */
class RabbitSimulator[B[_]](bindings: Bindings = IdentityBindings)(implicit M: Monad[B],
                                                                    X: MonadError[Future, Throwable],
                                                                   executionContext: ExecutionContext) extends AmqpClient[B, Future, Throwable, Unit] with StrictLogging {

  override implicit def monad: Monad[B] = M

  override implicit def effectMonad: MonadError[Future, Throwable] = X

  case class Publication(queueName: QueueName, message: Payload, consumeActionValue: Future[ConsumeAction])

  private val declarations = scala.collection.mutable.HashMap.empty[(ExchangeName, RoutingKey), QueueName]

  private val consumers = new scala.collection.mutable.HashMap[QueueName, List[Handler[Future, Delivery]]]()
  private val messagesBeingProcessed: TrieMap[UUID, Publication] = TrieMap.empty
  private val deliveryTag = new AtomicLong()


  def consumer(queueName: QueueName, handler: Handler[Future, Delivery], exceptionalAction: ConsumeAction = DeadLetter, prefetchCount: Int = 0): B[Unit] = monad.apply {
    val monitorHandler: Handler[Future, Delivery] = delivery => {
      val key = UUID.randomUUID()
      val consumeActionValue = handler(delivery)
      messagesBeingProcessed += key -> Publication(queueName, delivery.body, consumeActionValue)
      consumeActionValue.onComplete { _ =>
        val publication = messagesBeingProcessed(key)
        messagesBeingProcessed -= key
        logger.debug(s"Consume message [${publication.message.unmarshal[String]}] from ${publication.queueName}")
      }
      consumeActionValue
    }
    val handlers = monitorHandler :: consumers.getOrElse(queueName, List.empty)
    consumers += (queueName -> handlers)
  }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): B[Publisher[Future, PublishCommand]] =
    monad.apply {
      (command: PublishCommand) => {
        publish(command).map(_ => ())
      }
    }

  def publish(publishCommand: PublishCommand): Future[ConsumeAction] = {
    logger.debug(s"Publish message [${publishCommand.body.unmarshal[String]}] with ${publishCommand.exchange} ${publishCommand.routingKey}")
    if (isDefinedAt(publishCommand)) {
      val queueName = queueNameFor(publishCommand)
      consumers.get(queueName).fold(Future.failed[ConsumeAction](new RuntimeException(s"No consumers found for $queueName!"))) { handlers =>
        val responses = handlers.map(h => h(Delivery(publishCommand.body, ConsumerTag("ctag"), Envelope(deliveryTag.getAndIncrement(), false, publishCommand.exchange, publishCommand.routingKey), publishCommand.basicProperties)))
        Future.sequence(responses).map(actions => actions.find(_ != Ack).getOrElse(Ack))
      }
    } else
      Future.failed(new RuntimeException(s"No queue defined for ${publishCommand.exchange} ${publishCommand.routingKey}"))
  }

  def queueNameFor(publishCommand: PublishCommand): QueueName =
    declarations.get(publishCommand.exchange, publishCommand.routingKey).fold(bindings(publishCommand.routingKey))(identity)

  def isDefinedAt(publishCommand: PublishCommand): Boolean =
    declarations.isDefinedAt(publishCommand.exchange, publishCommand.routingKey) || bindings.isDefinedAt(publishCommand.routingKey)

  def watchQueue(queueName: QueueName): ListBuffer[Delivery] = {
    val messages = new ListBuffer[Delivery]()
    this.consumer(queueName, { delivery =>
      messages += delivery
      logger.debug(s"Watch queue consume message [$delivery]")
      Future.successful(Ack)
    })
    messages
  }

  def waitForMessagesToBeProcessed()(implicit timeout: Duration): Unit = {
    Await.result(Future.sequence(messagesBeingProcessed.values.map(_.consumeActionValue)), timeout)
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] =
    Try(thunk(new AmqpOps {
      override def declareExchange(exchange: Exchange): Try[Unit] = Try(())

      override def bindQueue(binding: Binding): Try[Unit] = Try {
        declarations += binding.exchangeName -> binding.routingKey -> binding.queueName
      }

      override def declareQueue(queue: Queue): Try[Unit] = Try(())

      override def purgeQueue(name: QueueName): Try[Unit] = Try(())

      override def bindExchange(binding: ExchangeBinding): Try[Unit] = Try(())
    }))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = {
    //FIXME: implement
    ???
  }


}


object RabbitSimulator {

  import PublishCommandBuilder._

  val stringPublishCommandBuilder = publishCommandBuilder(StringPayloadMarshaller)
  val defaultPublishCommandBuilder = stringPublishCommandBuilder using ExchangeName("")

}