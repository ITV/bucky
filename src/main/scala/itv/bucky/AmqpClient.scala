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
import scala.concurrent.{ExecutionContext, Promise}

case class PublishCommand(exchange: Exchange, routingKey: RoutingKey, basicProperties: BasicProperties, body: Blob) {
  def description = s"${exchange.value}:${routingKey.value} $body"
}

case class RoutingKey(value: String)

case class Exchange(value: String)

sealed trait ConsumeAction

case object Ack extends ConsumeAction

case object DeadLetter extends ConsumeAction

case object RequeueImmediately extends ConsumeAction

case class ConsumerTag(value: String)

object ConsumerTag {
  val pidAndHost: ConsumerTag = ConsumerTag(ManagementFactory.getRuntimeMXBean.getName)
}

class AmqpClient(channelFactory: Lifecycle[Channel], consumerTag: ConsumerTag = ConsumerTag.pidAndHost) extends StrictLogging {

  def consumer(queueName: String, handler: Handler[Blob], exceptionalAction: ConsumeAction = DeadLetter)(implicit executionContext: ExecutionContext): Lifecycle[Unit] = {
    for {
      channel <- channelFactory
    } yield {
      channel.basicConsume(queueName, false, consumerTag.value, new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          val bodyBlob = Blob(body)
          logger.debug("Received delivery with tag {}L on {}: {}", box(envelope.getDeliveryTag), queueName, bodyBlob)
          handler(Blob(body)).recover {
            case error =>
              logger.error(s"Unhandled exception processing delivery ${envelope.getDeliveryTag}L on $queueName", error)
              exceptionalAction
          }.map { action =>
            logger.debug("Responding with {} to delivery {}L on {}", action, box(envelope.getDeliveryTag), queueName)
            action match {
              case Ack => getChannel.basicAck(envelope.getDeliveryTag, false)
              case DeadLetter => getChannel.basicNack(envelope.getDeliveryTag, false, false)
              case RequeueImmediately => getChannel.basicNack(envelope.getDeliveryTag, false, true)
            }
          }
        }
      })
    }
  }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]] = {
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

  }

  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]

  private def publisherWrapperLifecycle(timeout: Duration): Lifecycle[Publisher[PublishCommand] => Publisher[PublishCommand]] = timeout match {
    case finiteTimeout: FiniteDuration =>
      ExecutorLifecycles.singleThreadScheduledExecutor.map(ec => new TimeoutPublisher(_, finiteTimeout)(ec))
    case infinite => NoOpLifecycle(identity)
  }
}