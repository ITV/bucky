package com.itv.bucky

import cats.effect.Sync
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope}

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

  def processDelivery[F[_]](channel: Channel,
                               queueName: QueueName,
                               handler: Handler[F, Delivery],
                               actionOnFailure: ConsumeAction,
                               delivery: Delivery)(implicit F: Sync[F]): F[Unit] =
      for {
        _ <- F.delay(logger.info("Received {} on {}", delivery, queueName))
        action <- handler(delivery).handleError { throwable =>
          logger.error(s"Handler responded with exception for delivery, will perform failure action of $actionOnFailure", throwable)
          actionOnFailure
        }
        _ <- F.delay(logger.info("Responding with {} to {} on {}", action, delivery, queueName))
      } yield action match {
        case Ack                => channel.basicAck(delivery.envelope.deliveryTag, false)
        case DeadLetter         => channel.basicNack(delivery.envelope.deliveryTag, false, false)
        case RequeueImmediately => requeueImmediately(channel, delivery)
      }

  def requeueImmediately[F[_], E](channel: Channel, delivery: Delivery): Unit =
    channel.basicNack(delivery.envelope.deliveryTag, false, true)

  def deliveryFrom(consumerTag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]) =
    Delivery(Payload(body),
             ConsumerTag(consumerTag),
             MessagePropertiesConverters(envelope),
             MessagePropertiesConverters(properties))

}
