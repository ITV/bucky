package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope, _}

object Consumer extends StrictLogging {

  def apply[F[_], E](channel: Channel, queueName: QueueName, consumer: Consumer, prefetchCount: Int = 0)(
      implicit M: MonadError[F, E]): Unit = {
    val consumerTag: ConsumerTag = ConsumerTag.create(queueName)
    logger.info(s"Starting consumer on $queueName with $consumerTag and a prefetchCount of ")
    Try {
      channel.basicQos(prefetchCount)
      channel.basicConsume(queueName.value, false, consumerTag.value, consumer)
    } match {
      case Success(_) => logger.info(s"Consumer on $queueName has been created!")
      case Failure(exception) =>
        logger.error(s"Failure when starting consumer on $queueName because ${exception.getMessage}", exception)
        throw exception
    }
  }

  def defaultConsumer[F[_], E](channel: Channel,
                               queueName: QueueName,
                               handler: Handler[F, Delivery],
                               actionOnFailure: ConsumeAction)(implicit F: MonadError[F, E]): Consumer =
    new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String,
                                  envelope: RabbitMQEnvelope,
                                  properties: BasicProperties,
                                  body: Array[Byte]): Unit = {
        val delivery = deliveryFrom(consumerTag, envelope, properties, body)
        Consumer.processDelivery(channel, queueName, handler, actionOnFailure, delivery)
      }
    }

  def processDelivery[E, F[_]](channel: Channel,
                               queueName: QueueName,
                               handler: Handler[F, Delivery],
                               actionOnFailure: ConsumeAction,
                               delivery: Delivery)(implicit F: MonadError[F, E]) =
    F.map {
      logger.debug("Received {} on {}", delivery, queueName)
      F.handleError(F.flatMap(F.apply(handler(delivery)))(identity)) { error =>
        logger.error(s"Unhandled exception processing delivery ${delivery.envelope.deliveryTag}L on $queueName", error)
        F.apply(actionOnFailure)
      }
    } { action =>
      logger.debug("Responding with {} to {} on {}", action, delivery, queueName)
      action match {
        case Ack                => channel.basicAck(delivery.envelope.deliveryTag, false)
        case DeadLetter         => channel.basicNack(delivery.envelope.deliveryTag, false, false)
        case RequeueImmediately => requeueImmediately(channel, delivery)
      }
    }

  def requeueImmediately[F[_], E](channel: Channel, delivery: Delivery): Unit =
    channel.basicNack(delivery.envelope.deliveryTag, false, true)

  def deliveryFrom(consumerTag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]) =
    Delivery(Payload(body),
             ConsumerTag(consumerTag),
             MessagePropertiesConverters(envelope),
             MessagePropertiesConverters(properties))

}
