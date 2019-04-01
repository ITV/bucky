package com.itv.bucky

import cats.effect.{ConcurrentEffect, Sync}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ConfirmListener, DefaultConsumer, Channel => RabbitChannel, Envelope => RabbitMQEnvelope}

object Consumer extends StrictLogging {

//  def apply[F[_], E](channel: Channel, queueName: QueueName, consumer: Consumer, prefetchCount: Int = 0): Unit = {
//    val consumerTag: ConsumerTag = ConsumerTag.create(queueName)
//    logger.info(s"Starting consumer on $queueName with $consumerTag and a prefetchCount of ")
//    Try {
//      channel.basicQos(prefetchCount)
//      channel.basicConsume(queueName.value, false, consumerTag.value, consumer)
//    } match {
//      case Success(_) => logger.info(s"Consumer on $queueName has been created!")
//      case Failure(exception) =>
//        logger.error(s"Failure when starting consumer on $queueName because ${exception.getMessage}", exception)
//        throw exception
//    }
//  }
//
//  def defaultConsumer[F[_]](channel: Channel,
//                               queueName: QueueName,
//                               handler: Handler[F, Delivery],
//                               actionOnFailure: ConsumeAction): Consumer =
//    new DefaultConsumer(channel) {
//      logger.info(s"Creating consumer for $queueName")
//      override def handleDelivery(consumerTag: String,
//                                  envelope: RabbitMQEnvelope,
//                                  properties: BasicProperties,
//                                  body: Array[Byte]): Unit = {
//        val delivery = deliveryFrom(consumerTag, envelope, properties, body)
//        Consumer.processDelivery(channel, queueName, handler, actionOnFailure, delivery)
//      }
//    }

  import cats.implicits._
  import cats.effect.implicits._

  def deliveryFrom(tag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]) =
    Delivery(Payload(body), ConsumerTag(tag), MessagePropertiesConverters(envelope), MessagePropertiesConverters(properties))

}
