package itv.bucky

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.rabbitmq.client.AMQP._
import com.rabbitmq.client.{Channel, Command, ConfirmListener, Connection, Consumer, Envelope, FlowListener, GetResponse, MessageProperties, Method, ReturnListener, ShutdownListener, ShutdownSignalException, BasicProperties => _}
import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}

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
class RabbitSimulator(bindings: Bindings = IdentityBindings)(implicit executionContext: ExecutionContext) extends AmqpClient with StrictLogging {

  case class Publication(queueName: QueueName, message: Payload, consumeActionValue: Future[ConsumeAction])

  private val consumers = new scala.collection.mutable.HashMap[QueueName, Handler[Delivery]]()
  private val messagesBeingProcessed: TrieMap[UUID, Publication] = TrieMap.empty
  private val deliveryTag = new AtomicLong()

  def consumer(queueName: QueueName, handler: Handler[Delivery], exceptionalAction: ConsumeAction = DeadLetter)(implicit executionContext: ExecutionContext): Lifecycle[Unit] = NoOpLifecycle {
    val monitorHandler: Handler[Delivery] = delivery => {
      val key = UUID.randomUUID()
      val consumeActionValue = handler(delivery)
      messagesBeingProcessed += key -> Publication(queueName, delivery.body, consumeActionValue)
      consumeActionValue.onComplete { _ =>
        val publication = messagesBeingProcessed(key)
        messagesBeingProcessed -= key
        logger.debug(s"Consume message [${publication.message.to[String]}] from ${publication.queueName}")
      }
      consumeActionValue
    }
    consumers += (queueName -> monitorHandler)
  }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]] =
    NoOpLifecycle[Publisher[PublishCommand]] {
      (command: PublishCommand) => {
        publish(command.body)(command.routingKey).map(_ => ())
      }
    }

  def publish(message: Payload)(routingKey: RoutingKey, headers: Map[String, AnyRef] = Map.empty[String, AnyRef]): Future[ConsumeAction] = {
    logger.debug(s"Publish message [${message.to[String]}] with $routingKey")
    if (bindings.isDefinedAt(routingKey)) {
      val queueName = bindings(routingKey)
      consumers.get(queueName).fold(Future.failed[ConsumeAction](new RuntimeException(s"No consumers found for $queueName!"))) { handler =>
        import scala.collection.convert.wrapAll._
        handler(Delivery(message, ConsumerTag("ctag"), new Envelope(deliveryTag.getAndIncrement(), false, "", routingKey.value),
          new BasicProperties.Builder()
            .contentType(MessageProperties.PERSISTENT_BASIC.getContentType)
            .deliveryMode(2)
            .priority(0)
            .headers(mapAsJavaMap(headers))
            .build()))
      }
    } else
      Future.failed(new RuntimeException("No queue defined for" + routingKey))
  }

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

  override def withChannel[T](thunk: Channel => T): T = thunk(new Channel {

      override def queueDeclarePassive(queue: String): Queue.DeclareOk = new Queue.DeclareOk.Builder().build()

      override def flowBlocked(): Boolean = false

      override def getDefaultConsumer: Consumer = ???

      override def getNextPublishSeqNo: Long = 0

      override def basicPublish(exchange: String, routingKey: String, props: BasicProperties, body: Array[Byte]): Unit = {}

      override def basicPublish(exchange: String, routingKey: String, mandatory: Boolean, props: BasicProperties, body: Array[Byte]): Unit = {}

      override def basicPublish(exchange: String, routingKey: String, mandatory: Boolean, immediate: Boolean, props: BasicProperties, body: Array[Byte]): Unit = {}

      override def rpc(method: Method): Command = ???

      override def addReturnListener(listener: ReturnListener):Unit = {}

      override def basicCancel(consumerTag: String):Unit = {}

      override def waitForConfirmsOrDie():Unit = {}

      override def waitForConfirmsOrDie(timeout: Long):Unit = {}

      override def basicAck(deliveryTag: Long, multiple: Boolean):Unit = {}

      override def basicNack(deliveryTag: Long, multiple: Boolean, requeue: Boolean):Unit = {}

      override def getChannelNumber: Int = 0

      override def exchangeBind(destination: String, source: String, routingKey: String): Exchange.BindOk = ???

      override def exchangeBind(destination: String, source: String, routingKey: String, arguments: util.Map[String, AnyRef]): Exchange.BindOk = ???

      override def waitForConfirms(): Boolean = false

      override def waitForConfirms(timeout: Long): Boolean = false

      override def queueDeclare(): Queue.DeclareOk = new Queue.DeclareOk.Builder().build()

      override def queueDeclare(queue: String, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, arguments: util.Map[String, AnyRef]): Queue.DeclareOk = new Queue.DeclareOk.Builder().build()

      override def clearFlowListeners():Unit = {}

      override def basicReject(deliveryTag: Long, requeue: Boolean):Unit = {}

      override def clearConfirmListeners():Unit = {}

      override def exchangeUnbind(destination: String, source: String, routingKey: String): Exchange.UnbindOk = new Exchange.UnbindOk.Builder().build()

      override def exchangeUnbind(destination: String, source: String, routingKey: String, arguments: util.Map[String, AnyRef]): Exchange.UnbindOk = new Exchange.UnbindOk.Builder().build()

      override def exchangeDeclare(exchange: String, `type`: String): Exchange.DeclareOk = new Exchange.DeclareOk.Builder().build()

      override def exchangeDeclare(exchange: String, `type`: String, durable: Boolean): Exchange.DeclareOk = new Exchange.DeclareOk.Builder().build()

      override def exchangeDeclare(exchange: String, `type`: String, durable: Boolean, autoDelete: Boolean, arguments: util.Map[String, AnyRef]): Exchange.DeclareOk = new Exchange.DeclareOk.Builder().build()

      override def exchangeDeclare(exchange: String, `type`: String, durable: Boolean, autoDelete: Boolean, internal: Boolean, arguments: util.Map[String, AnyRef]): Exchange.DeclareOk = new Exchange.DeclareOk.Builder().build()

      override def txSelect(): Tx .SelectOk = ???

      override def basicGet(queue: String, autoAck: Boolean): GetResponse = ???

      override def removeReturnListener(listener: ReturnListener): Boolean =  false

      override def addFlowListener(listener: FlowListener):Unit = {}

      override def basicRecoverAsync(requeue: Boolean):Unit = {}

      override def queuePurge(queue: String): Queue .PurgeOk = ???

      override def queueUnbind(queue: String, exchange: String, routingKey: String): Queue .UnbindOk = ???

      override def queueUnbind(queue: String, exchange: String, routingKey: String, arguments: util.Map[String, AnyRef]): Queue .UnbindOk = ???

      override def exchangeDelete(exchange: String, ifUnused: Boolean): Exchange .DeleteOk = ???

      override def exchangeDelete(exchange: String): Exchange .DeleteOk = ???

      override def confirmSelect(): Confirm .SelectOk = ???

      override def removeConfirmListener(listener: ConfirmListener): Boolean = false

      override def basicConsume(queue: String, callback: Consumer): String = queue

      override def basicConsume(queue: String, autoAck: Boolean, callback: Consumer): String = queue

      override def basicConsume(queue: String, autoAck: Boolean, arguments: util.Map[String, AnyRef], callback: Consumer): String = queue

      override def basicConsume(queue: String, autoAck: Boolean, consumerTag: String, callback: Consumer): String = queue

      override def basicConsume(queue: String, autoAck: Boolean, consumerTag: String, noLocal: Boolean, exclusive: Boolean, arguments: util.Map[String, AnyRef], callback: Consumer): String = queue

      override def abort():Unit = {}

      override def abort(closeCode: Int, closeMessage: String):Unit = {}

      override def close():Unit = {}

      override def close(closeCode: Int, closeMessage: String):Unit = {}

      override def setDefaultConsumer(consumer: Consumer):Unit = {}

      override def queueBind(queue: String, exchange: String, routingKey: String): Queue.BindOk = ???

      override def queueBind(queue: String, exchange: String, routingKey: String, arguments: util.Map[String, AnyRef]): Queue .BindOk = ???

      override def txCommit(): Tx .CommitOk = ???

      override def exchangeDeclarePassive(name: String): Exchange.DeclareOk = ???

      override def basicRecover(): Basic .RecoverOk = ???

      override def basicRecover(requeue: Boolean): Basic .RecoverOk = ???

      override def addConfirmListener(listener: ConfirmListener):Unit = {}

      override def basicQos(prefetchSize: Int, prefetchCount: Int, global: Boolean):Unit = {}

      override def basicQos(prefetchCount: Int, global: Boolean):Unit = {}

      override def basicQos(prefetchCount: Int):Unit = {}

      override def asyncRpc(method: Method):Unit = {}

      override def queueDelete(queue: String): Queue .DeleteOk = ???

      override def queueDelete(queue: String, ifUnused: Boolean, ifEmpty: Boolean): Queue .DeleteOk = ???

      override def removeFlowListener(listener: FlowListener): Boolean = false

      override def getConnection: Connection = ???

      override def txRollback(): Tx .RollbackOk = ???

      override def clearReturnListeners():Unit = {}

      override def removeShutdownListener(listener: ShutdownListener):Unit = {}

      override def isOpen: Boolean = false

      override def notifyListeners():Unit = {}

      override def getCloseReason: ShutdownSignalException = ???

      override def addShutdownListener(listener: ShutdownListener):Unit = {}
    })

}
