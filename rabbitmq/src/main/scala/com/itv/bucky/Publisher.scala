package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import com.rabbitmq.client._

object Publisher extends StrictLogging {

  def publish[T](channel: Channel,
                 cmd: PublishCommand,
                 pendingConfirmation: T,
                 pendingConfirmations: PendingConfirmations[T])(fail: (T, Exception) => Unit): Unit =
    channel.synchronized {
      logger.debug(s"Acquire the channel: $channel")
      val deliveryTag = channel.getNextPublishSeqNo
      logger.debug("Publishing with delivery tag {}L to {}:{} with {}: {}",
                   box(deliveryTag),
                   cmd.exchange,
                   cmd.routingKey,
                   cmd.basicProperties,
                   cmd.body)
      pendingConfirmations.addPendingConfirmation(deliveryTag, pendingConfirmation)
      try {
        channel.basicPublish(cmd.exchange.value,
                             cmd.routingKey.value,
                             false,
                             false,
                             MessagePropertiesConverters(cmd.basicProperties),
                             cmd.body.value)
      } catch {
        case exception: Exception =>
          logger.error(s"Failed to publish message with delivery tag ${deliveryTag}L to ${cmd.description}", exception)
          pendingConfirmations.completeConfirmation(deliveryTag)(t => fail(t, exception))

      }
      logger.debug(s"Release the channel: $channel")
    }

  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]

  case class PendingConfirmations[T](
      unconfirmedPublications: java.util.TreeMap[Long, List[T]] = new java.util.TreeMap[Long, List[T]]()) {

    import scala.collection.JavaConverters._

    def completeConfirmation(deliveryTag: Long, multiple: Boolean)(complete: T => Unit): Unit =
      pop(deliveryTag, multiple).foreach(complete)

    def completeConfirmation(deliveryTag: Long)(complete: T => Unit): Unit = {
      val nonMultipleConfirmation = pop(deliveryTag, multiple = false)
      val confirmationToComplete =
        if (nonMultipleConfirmation.isEmpty)
          pop(deliveryTag, multiple = false)
        else
          nonMultipleConfirmation
      confirmationToComplete.foreach(complete)
    }

    def addPendingConfirmation(deliveryTag: Long, pendingConfirmation: T) =
      unconfirmedPublications.synchronized {
        unconfirmedPublications.put(
          deliveryTag,
          Option(unconfirmedPublications.get(deliveryTag)).toList.flatten.+:(pendingConfirmation))
      }

    private def pop(deliveryTag: Long, multiple: Boolean) =
      unconfirmedPublications.synchronized {
        if (multiple) {
          val entries       = unconfirmedPublications.headMap(deliveryTag + 1L)
          val removedValues = entries.values().asScala.toList
          entries.clear()
          removedValues.flatten
        } else {
          Option(unconfirmedPublications.remove(deliveryTag)).toList.flatten
        }
      }

  }

  def confirmListener[T](channel: Channel)(success: T => Unit)(
      fail: (T, Exception) => Unit): PendingConfirmations[T] = {
    channel.confirmSelect()
    val pendingConfirmations = new PendingConfirmations[T]()
    logger.info(s"Create confirm listener for channel $channel")
    channel.addConfirmListener(new ConfirmListener {
      override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
        logger.debug("Publish acknowledged with delivery tag {}L, multiple = {}", box(deliveryTag), box(multiple))
        pendingConfirmations.completeConfirmation(deliveryTag, multiple)(success)
      }

      override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
        logger.error("Publish negatively acknowledged with delivery tag {}L, multiple = {}",
                     box(deliveryTag),
                     box(multiple))
        pendingConfirmations.completeConfirmation(deliveryTag, multiple)(
          fail(_, new RuntimeException("AMQP server returned Nack for publication")))
      }
    })
    pendingConfirmations
  }

}
