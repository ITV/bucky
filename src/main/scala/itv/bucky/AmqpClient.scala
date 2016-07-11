package itv.bucky

import java.util.concurrent.TimeUnit

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._
import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.{ExecutorLifecycles, Lifecycle, NoOpLifecycle}

import scala.collection.convert.wrapAsScala.collectionAsScalaIterable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait AmqpClient {

  def publisherOf[T](builder: PublishCommandBuilder[T], timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS))
                    (implicit executionContext: ExecutionContext): Lifecycle[Publisher[T]] =
    publisher(timeout).map(AmqpClient.publisherOf(builder))

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]]

  def consumer(queueName: QueueName, handler: Handler[Delivery], exceptionalAction: ConsumeAction = DeadLetter)
              (implicit executionContext: ExecutionContext): Lifecycle[Unit]

  def withChannel[T](thunk: Channel => T): T

}

class RawAmqpClient(channelFactory: Lifecycle[Channel], consumerTag: ConsumerTag = ConsumerTag.pidAndHost) extends AmqpClient with StrictLogging {

  def consumer(queueName: QueueName, handler: Handler[Delivery], actionOnFailure: ConsumeAction = DeadLetter)
              (implicit executionContext: ExecutionContext): Lifecycle[Unit] =
    for {
      channel <- channelFactory
    } yield {
      logger.info(s"Starting consumer on $queueName")
      channel.basicConsume(queueName.value, false, consumerTag.value, new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          val delivery = Delivery(Payload(body), ConsumerTag(consumerTag), envelope, properties)
          logger.debug("Received {} on {}", delivery, queueName)
          safePerform(handler(delivery)).onComplete { result =>
            val action = result match {
              case Success(outcome) => outcome
              case Failure(error) =>
                logger.error(s"Unhandled exception processing delivery ${envelope.getDeliveryTag}L on $queueName", error)
                actionOnFailure
            }
            logger.debug("Responding with {} to {} on {}", action, delivery, queueName)
            action match {
              case Ack => getChannel.basicAck(envelope.getDeliveryTag, false)
              case DeadLetter => getChannel.basicNack(envelope.getDeliveryTag, false, false)
              case RequeueImmediately => getChannel.basicNack(envelope.getDeliveryTag, false, true)
            }
          }
        }
      })
    }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]] =
    for {
      channel <- channelFactory
      publisherWrapper <- publisherWrapperLifecycle(timeout)
    } yield {
      val unconfirmedPublications = new java.util.TreeMap[Long, Promise[Unit]]()
      channel.confirmSelect()
      channel.addConfirmListener(new ConfirmListener {
        override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
          logger.debug("Publish acknowledged with delivery tag {}L, multiple = {}", box(deliveryTag), box(multiple))
          removePromises(deliveryTag, multiple).foreach(_.success(()))
        }

        override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
          logger.error("Publish negatively acknowledged with delivery tag {}L, multiple = {}", box(deliveryTag), box(multiple))
          removePromises(deliveryTag, multiple).foreach(_.failure(new RuntimeException("AMQP server returned Nack for publication")))
        }

        private def removePromises(deliveryTag: Long, multiple: Boolean): List[Promise[Unit]] = channel.synchronized {
          if (multiple) {
            val entries = unconfirmedPublications.headMap(deliveryTag + 1L)
            val removedValues = collectionAsScalaIterable(entries.values()).toList
            entries.clear()
            removedValues
          } else {
            Option(unconfirmedPublications.remove(deliveryTag)).toList
          }
        }
      })

      publisherWrapper(cmd => channel.synchronized {
        val promise = Promise[Unit]()
        val deliveryTag = channel.getNextPublishSeqNo
        logger.debug("Publishing with delivery tag {}L to {}:{} with {}: {}", box(deliveryTag), cmd.exchange, cmd.routingKey, cmd.basicProperties, cmd.body)
        unconfirmedPublications.put(deliveryTag, promise)
        try {
          channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, cmd.basicProperties, cmd.body.value)
        } catch {
          case exception: Exception =>
            logger.error(s"Failed to publish message with delivery tag ${deliveryTag}L to ${cmd.description}", exception)
            unconfirmedPublications.remove(deliveryTag).failure(exception)
        }
        promise.future
      })
    }

  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]

  private def publisherWrapperLifecycle(timeout: Duration): Lifecycle[Publisher[PublishCommand] => Publisher[PublishCommand]] = timeout match {
    case finiteTimeout: FiniteDuration =>
      ExecutorLifecycles.singleThreadScheduledExecutor.map(ec => new TimeoutPublisher(_, finiteTimeout)(ec))
    case infinite => NoOpLifecycle(identity)
  }

  override def withChannel[T](thunk: (Channel) => T): T =
    Lifecycle.using(channelFactory)(thunk)

}

object AmqpClient extends StrictLogging {

  def publisherOf[T](commandBuilder: PublishCommandBuilder[T])(publisher: Publisher[PublishCommand])
                    (implicit executionContext: ExecutionContext): Publisher[T] = (message: T) =>
    for {
      publishCommand <- Future {
        commandBuilder.toPublishCommand(message)
      }
      _ <- publisher(publishCommand)
    } yield ()

  def handlerOf[T](handler: Handler[T], deserializer: PayloadUnmarshaller[T], unmarshalFailureAction: ConsumeAction = DeadLetter)
                  (implicit ec: ExecutionContext): Handler[Delivery] =
    new PayloadUnmarshalHandler[T, ConsumeAction](deserializer)(handler, unmarshalFailureAction)

}