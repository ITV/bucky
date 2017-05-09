package com.itv.bucky.future

import java.util.concurrent.{Executors, TimeUnit}

import com.itv.bucky.{Envelope => _, _}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope, _}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

abstract class FutureAmqpClient[M[_]](channelFactory: M[Channel])(implicit M: Monad[M], executionContext: ExecutionContext) extends AmqpClient[M, Future, Throwable, Unit] with StrictLogging {

  override def consumer(queueName: QueueName, handler: Handler[Future, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): M[Unit] =
    M.flatMap(channelFactory) { (channel: Channel) =>
      M.apply(IdConsumer[Future, Throwable](channel, queueName, handler, actionOnFailure, prefetchCount) { _ => () })
    }

  import scala.collection.JavaConverters._

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): M[Publisher[Future, PublishCommand]] =
    M.flatMap(channelFactory) { channel: Channel =>
      M.apply(
        Try {
          logger.info(s"Creating publisher")
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
                val removedValues = entries.values().asScala.toList
                entries.clear()
                removedValues
              } else {
                Option(unconfirmedPublications.remove(deliveryTag)).toList
              }
            }
          })

          publisherWrapperLifecycle(timeout)(cmd => channel.synchronized {
            val promise = Promise[Unit]()
            val deliveryTag = channel.getNextPublishSeqNo
            logger.debug("Publishing with delivery tag {}L to {}:{} with {}: {}", box(deliveryTag), cmd.exchange, cmd.routingKey, cmd.basicProperties, cmd.body)
            unconfirmedPublications.put(deliveryTag, promise)
            try {
              channel.basicPublish(cmd.exchange.value, cmd.routingKey.value, false, false, MessagePropertiesConverters(cmd.basicProperties), cmd.body.value)
            } catch {
              case exception: Exception =>
                logger.error(s"Failed to publish message with delivery tag ${
                  deliveryTag
                }L to ${
                  cmd.description
                }", exception)
                unconfirmedPublications.remove(deliveryTag).failure(exception)
            }
            promise.future
          })
        } match {
          case Success(publisher) =>
            logger.info(s"Publisher has been created successfully!")
            publisher
          case Failure(exception) =>
            logger.error(s"Error when creating publisher because ${exception.getMessage}", exception)
            throw exception

        }
      )
    }

  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]

  private def publisherWrapperLifecycle(timeout: Duration): Publisher[Future, PublishCommand] => Publisher[Future, PublishCommand] = timeout match {
    case finiteTimeout: FiniteDuration =>
      new FutureTimeoutPublisher(_, finiteTimeout)(Executors.newSingleThreadScheduledExecutor())
    case _ => identity
  }
}
