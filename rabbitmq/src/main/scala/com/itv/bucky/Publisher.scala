package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import com.rabbitmq.client._

import scala.collection.immutable.TreeMap

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

  class PendingConfirmations[T] {

    import AtomicRef._
    private val unconfirmedPublicationsRef = Ref[TreeMap[Long, List[T]]](TreeMap.empty)

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

    def addPendingConfirmation(deliveryTag: Long, pendingConfirmation: T): Unit =
      unconfirmedPublicationsRef.update { x =>
        x + (deliveryTag -> x.get(deliveryTag).toList.flatten.+:(pendingConfirmation))
      }

    private def pop(deliveryTag: Long, multiple: Boolean): List[T] =
      unconfirmedPublicationsRef.modify { x =>
        if (multiple) {
          val entries = x.until(deliveryTag + 1L).toList
          entries.flatMap(_._2).toList -> (x -- entries.map(_._1))

        } else {
          x.get(deliveryTag).toList.flatten -> (x - deliveryTag)
        }
      }._1

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
