package itv.bucky

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}
import itv.utils.Blob

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}

case class QueueName(value: String)

object ExchangeSimulator {

  type ExchangeSimulator = PartialFunction[RoutingKey, QueueName]


  case object IdentityExchange extends ExchangeSimulator {
    def apply(routingQueue: RoutingKey): QueueName = QueueName(routingQueue.value)

    override def isDefinedAt(key: RoutingKey): Boolean = true
  }

  case class MapExchange(mappings: (RoutingKey, QueueName)*) extends ExchangeSimulator {
    def apply(routingQueue: RoutingKey): QueueName = mappings.toMap.apply(routingQueue)

    override def isDefinedAt(key: RoutingKey): Boolean = mappings.toMap.isDefinedAt(key)
  }

}

import ExchangeSimulator._

/**
  * Provides an AmqpClient implementation that simulates RabbitMQ server with one main difference:
  * Messages are sent directly to the consumer when published, there is no intermediate queue.
  * This makes it easy for tests to publish a message and see the corresponding ConsumeAction, e.g. Ack, Nack or Requeue.
  * Tests can use `RabbitSimulator.watchQueue` to see what messages get published to a queue that the application doesn't consume from.
  * @param exchange A mapping from routing key to queue name, defaults to identity.
  */
class RabbitSimulator(exchange: ExchangeSimulator = IdentityExchange)(implicit executionContext: ExecutionContext) extends AmqpClient with StrictLogging {

  case class Publication(queueName: QueueName, message: Blob, consumeActionValue: Future[ConsumeAction])

  private val consumers = new scala.collection.mutable.HashMap[QueueName, Handler[Blob]]()
  private val messagesBeingProcessed: TrieMap[UUID, Publication] = TrieMap.empty

  def consumer(queueName: String, handler: Handler[Blob], exceptionalAction: ConsumeAction = DeadLetter)(implicit executionContext: ExecutionContext): Lifecycle[Unit] = NoOpLifecycle {
    val monitorHandler: Handler[Blob] = message => {
      val key = UUID.randomUUID()
      val consumeActionValue = handler(message)
      messagesBeingProcessed += key -> Publication(QueueName(queueName), message, consumeActionValue)
      consumeActionValue.onComplete { _ =>
        val publication = messagesBeingProcessed(key)
        messagesBeingProcessed -= key
        logger.debug(s"Consume message [${publication.message.to[String]}] from ${publication.queueName}")
      }
      consumeActionValue
    }
    consumers += (QueueName(queueName) -> monitorHandler)
  }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS))(implicit executionContext: ExecutionContext): Lifecycle[Publisher[PublishCommand]] =
    NoOpLifecycle[Publisher[PublishCommand]] {
      (command: PublishCommand) => {
        publish(command.body)(command.routingKey).map(_ => ())
      }
    }

  def publish(message: Blob)(routingKey: RoutingKey): Future[ConsumeAction] = {
    logger.debug(s"Publish message [${message.to[String]}] with $routingKey")
    if (exchange.isDefinedAt(routingKey)) {
      val queueName = exchange(routingKey)
      consumers.get(queueName).fold(Future.failed[ConsumeAction](new RuntimeException(s"No consumers found for $queueName!"))) { handler =>
        handler(message)
      }
    } else
      Future.failed(new RuntimeException("No queue defined for" + routingKey))
  }

  def watchQueue(queueName: String)(implicit executionContext: ExecutionContext): ListBuffer[Blob] = {
    val messages = new ListBuffer[Blob]()
    this.consumer(queueName, { message =>
      messages += message
      Future.successful(Ack)
    })
    messages
  }

  def waitForMessagesToBeProcessed()(implicit timeout: Duration): Unit = {
    Await.result(Future.sequence(messagesBeingProcessed.values.map(_.consumeActionValue)), timeout)
  }
}
