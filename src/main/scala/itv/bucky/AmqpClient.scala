package itv.bucky

import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._
import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.{ExecutorLifecycles, Lifecycle, NoOpLifecycle}
import itv.utils.Blob

import scala.collection.convert.wrapAsScala.collectionAsScalaIterable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

trait AmqpClient {
  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS))(implicit executionContext: ExecutionContext): Lifecycle[Publisher[PublishCommand]]

  def consumer(queueName: String, handler: Handler[Delivery], exceptionalAction: ConsumeAction = DeadLetter)(implicit executionContext: ExecutionContext): Lifecycle[Unit]
}


class RawAmqpClient(channelFactory: Lifecycle[Channel], consumerTag: ConsumerTag = ConsumerTag.pidAndHost) extends AmqpClient with StrictLogging {

  def consumer(queueName: String, handler: Handler[Delivery], exceptionalAction: ConsumeAction = DeadLetter)(implicit executionContext: ExecutionContext): Lifecycle[Unit] =
    for {
      channel <- channelFactory
    } yield {
      channel.basicConsume(queueName, false, consumerTag.value, new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          val delivery = Delivery(Blob(body), ConsumerTag(consumerTag), envelope, properties)
          logger.debug("Received {} on {}", delivery, queueName)
          handler(delivery).recover {
            case error =>
              logger.error(s"Unhandled exception processing delivery ${envelope.getDeliveryTag}L on $queueName", error)
              exceptionalAction
          }.map { action =>
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

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS))(implicit executionContext: ExecutionContext): Lifecycle[Publisher[PublishCommand]] =
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
        logger.debug("Publishing with delivery tag {}L to {}:{} {}", box(deliveryTag), cmd.exchange, cmd.routingKey, cmd.body)
        unconfirmedPublications.put(deliveryTag, promise)
        try {
          channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, cmd.basicProperties, cmd.body.content)
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


}


object AmqpClient extends StrictLogging {
  def publisherOf[T](publisher: Publisher[PublishCommand])(implicit executionContext: ExecutionContext, serializer: PublishCommandSerializer[T]): Publisher[T] = (message: T) =>
    for {
      publishCommand <- Future {
        serializer.toPublishCommand(message)
      }
      _ <- publisher(publishCommand)
    } yield ()


  def handlerOf[T](handler: Handler[T], deserializationFailureAction: ConsumeAction = DeadLetter)(implicit ec: ExecutionContext, deserializer: BlobDeserializer[T]): Handler[Delivery] =
    new Handler[Delivery] {
      override def apply(delivery: Delivery): Future[ConsumeAction] = Future(deserializer(delivery.body)).flatMap {
        case DeserializerResult.Success(message) => handler(message)
        case DeserializerResult.Failure(reason) =>
          logger.error("Cannot deserialize: {} because: \"{}\" (will {})", delivery.body, reason, deserializationFailureAction)
          Future.successful(deserializationFailureAction)
      }.recoverWith { case error: Throwable =>
        logger.error("Cannot deserialize: {} because: \"{}\" (will {})", delivery.body, error.getMessage, deserializationFailureAction, error)
        Future.successful(deserializationFailureAction)
      }
    }
}