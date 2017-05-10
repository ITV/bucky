package com.itv.bucky

import com.itv.bucky.Monad.Id
import com.itv.bucky.decl.{Binding, Exchange, Queue}


import com.typesafe.scalalogging.StrictLogging


import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

import com.itv.bucky.{Envelope => _, _}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope, _}
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}

object IdConsumer extends StrictLogging {

  def apply[M[_], E](channel: Channel, queueName: QueueName, handler: Handler[M, Delivery], actionOnFailure: ConsumeAction, prefetchCount: Int = 0)
                    (f: M[_] => Unit)(implicit M: MonadError[M, E]): Unit = {
    val consumerTag: ConsumerTag = ConsumerTag.create(queueName)
    logger.info(s"Starting consumer on $queueName with $consumerTag and a prefetchCount of ")
    Try {
      channel.basicQos(prefetchCount)
      channel.basicConsume(queueName.value, false, consumerTag.value, RabbitConsumer(channel, queueName, handler, actionOnFailure)(f))
    } match {
      case Success(_) => logger.info(s"Consumer on $queueName has been created!")
      case Failure(exception) =>
        logger.error(s"Failure when starting consumer on $queueName because ${exception.getMessage}", exception)
        throw exception
    }
  }
}

object RabbitConsumer extends StrictLogging {
  def apply[M[_], E](channel: Channel, queueName: QueueName, handler: Handler[M, Delivery], actionOnFailure: ConsumeAction)(f: M[_] => Unit)(implicit M: MonadError[M, E]): Consumer = {
    new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        val delivery = Delivery(Payload(body), ConsumerTag(consumerTag), MessagePropertiesConverters(envelope), MessagePropertiesConverters(properties))
        logger.debug("Received {} on {}", delivery, queueName)
        f(M.map(M.handleError(M.flatMap(M.apply(handler(delivery)))(identity)) { error =>
          logger.error(s"Unhandled exception processing delivery ${envelope.getDeliveryTag}L on $queueName", error)
          M.apply(actionOnFailure)
        }) { action =>
          logger.debug("Responding with {} to {} on {}", action, delivery, queueName)
          action match {
            case Ack => channel.basicAck(envelope.getDeliveryTag, false)
            case DeadLetter => channel.basicNack(envelope.getDeliveryTag, false, false)
            case RequeueImmediately => channel.basicNack(envelope.getDeliveryTag, false, true)
          }
        })
      }
    }
  }
}

object IdConnection extends StrictLogging {
  def apply(config: AmqpClientConfig): Id[Connection] = {
    Try {
      logger.info(s"Starting AmqpClient")
      val connectionFactory = new ConnectionFactory()
      connectionFactory.setHost(config.host)
      connectionFactory.setPort(config.port)
      connectionFactory.setUsername(config.username)
      connectionFactory.setPassword(config.password)
      connectionFactory.setAutomaticRecoveryEnabled(config.networkRecoveryInterval.isDefined)
      config.networkRecoveryInterval.map(_.toMillis.toInt).foreach(connectionFactory.setNetworkRecoveryInterval)
      config.virtualHost.foreach(connectionFactory.setVirtualHost)
      connectionFactory.newConnection()
    } match {
      case Success(connection) =>
        logger.info(s"AmqpClient has been started successfully!")
        connection
      case Failure(exception) =>
        logger.error(s"Failure when starting AmqpClient because ${exception.getMessage}", exception)
        throw exception
    }
  }

  def close(connection: Connection): Unit =
    if (connection.isOpen) {
      connection.close()
    }
}

object IdChannel extends StrictLogging {
  /**
    * Close channel and connection associated with the IdAmqpClient
    *
    * Note: This should not be used if you share the connection
    *
    * @param channel the channel
    */
  def closeAll(channel: Channel): Unit = {
    logger.info(s"Closing channel and conection for $channel")
    val connection = channel.getConnection
    IdChannel.close(channel)
    IdConnection.close(connection)
  }


  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]


  case class PendingConfirmations[T](unconfirmedPublications: java.util.TreeMap[Long, List[T]] = new java.util.TreeMap[Long, List[T]]()) {

    import scala.collection.JavaConverters._

    def completeConfirmation(deliveryTag: Long, multiple: Boolean)(complete: T => Unit): Unit =
      pop(deliveryTag, multiple).foreach(complete)


    def completeConfirmation(deliveryTag: Long)(complete: T => Unit): Unit = {
      val result = pop(deliveryTag, multiple = false)
      val toComplete = if (result.isEmpty)
        pop(deliveryTag, multiple = false)
      else
        result
      toComplete.foreach(complete)
    }

    def addPendingConfirmation(deliveryTag: Long, register: T) = {
      unconfirmedPublications.synchronized {
        unconfirmedPublications.put(deliveryTag, Option(unconfirmedPublications.get(deliveryTag)).toList.flatten.+:(register))
      }
    }

    private def pop[T](deliveryTag: Long, multiple: Boolean) = {
      if (multiple) {
        val entries = unconfirmedPublications.headMap(deliveryTag + 1L)
        val removedValues = entries.values().asScala.toList
        entries.clear()
        removedValues.flatten
      }
      else {
        Option(unconfirmedPublications.remove(deliveryTag)).toList.flatten
      }
    }
  }

  def confirmListener[T](channel: Channel)(success: T => Unit)(failure: T => Unit): PendingConfirmations[T] = {
    channel.confirmSelect()
    val pendingConfirmations = new PendingConfirmations[T]()
    logger.info(s"Create confirm listener for channel $channel")
    channel.addConfirmListener(new ConfirmListener {
      override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
        logger.debug("Publish acknowledged with delivery tag {}L, multiple = {}", box(deliveryTag), box(multiple))
        pendingConfirmations.completeConfirmation(deliveryTag, multiple)(success)
      }

      override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
        logger.error("Publish negatively acknowledged with delivery tag {}L, multiple = {}", box(deliveryTag), box(multiple))
        pendingConfirmations.completeConfirmation(deliveryTag, multiple)(failure)
      }
    })
    pendingConfirmations
  }

  def close(channel: Channel): Unit =
    if (channel.getConnection.isOpen) {
      channel.close()
    }

  def estimateMessageCount(channel: Channel, queueName: QueueName) =
    Try(Option(channel.basicGet(queueName.value, false)).fold(0)(_.getMessageCount + 1))


  def apply(connection: Connection): Id[Channel] = {
    Try {
      logger.info(s"Starting Channel")
      connection.createChannel()
    } match {
      case Success(channel) =>
        logger.info(s"Channel has been started successfully!")
        channel
      case Failure(exception) =>
        logger.error(s"Failure when starting Channel because ${exception.getMessage}", exception)
        throw exception
    }
  }
}

case class ChannelAmqpOps(channel: Channel) extends AmqpOps {

  import scala.collection.JavaConverters._

  override def declareExchange(exchange: Exchange): Try[Unit] = Try {
    channel.exchangeDeclare(
      exchange.name.value,
      exchange.exchangeType.value,
      exchange.isDurable,
      exchange.shouldAutoDelete,
      exchange.isInternal,
      exchange.arguments.asJava)
  }

  override def bindQueue(binding: Binding): Try[Unit] = Try {
    channel.queueBind(
      binding.queueName.value,
      binding.exchangeName.value,
      binding.routingKey.value,
      binding.arguments.asJava)
  }

  override def declareQueue(queue: Queue): Try[Unit] = Try {
    channel.queueDeclare(
      queue.name.value,
      queue.isDurable,
      queue.isExclusive,
      queue.shouldAutoDelete,
      queue.arguments.asJava)
  }

  override def purgeQueue(name: QueueName): Try[Unit] = Try {
    channel.queuePurge(name.value)
  }
}
