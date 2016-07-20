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
import com.rabbitmq.client.{Envelope => RabbitMQEnvelope}
import itv.bucky.decl.{Binding, Exchange, Queue}

class RawAmqpClient(channelFactory: Lifecycle[Channel], consumerTag: ConsumerTag = ConsumerTag.pidAndHost) extends AmqpClient with StrictLogging {

  def consumer(queueName: QueueName, handler: Handler[Delivery], actionOnFailure: ConsumeAction = DeadLetter)
              (implicit executionContext: ExecutionContext): Lifecycle[Unit] =
    for {
      channel <- channelFactory
    } yield {
      logger.info(s"Starting consumer on $queueName")
      Try {
        channel.basicConsume(queueName.value, false, consumerTag.value, new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String, envelope: RabbitMQEnvelope, properties: BasicProperties, body: Array[Byte]): Unit = {
            val delivery = Delivery(Payload(body), ConsumerTag(consumerTag), MessagePropertiesConverters(envelope), MessagePropertiesConverters(properties))
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
      } match {
        case Success(_) => logger.info(s"Consumer on $queueName has been created!")
        case Failure(exception) =>
          logger.error(s"Failure when starting consumer on $queueName because ${exception.getMessage}", exception)
          throw exception
      }

    }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): Lifecycle[Publisher[PublishCommand]] =
    for {
      channel <- channelFactory
      publisherWrapper <- publisherWrapperLifecycle(timeout)
    } yield {
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
    }

  // Unfortunately explicit boxing seems necessary due to Scala inferring logger varargs as being of type AnyRef*
  @inline private def box(x: AnyVal): AnyRef = x.asInstanceOf[AnyRef]

  private def publisherWrapperLifecycle(timeout: Duration): Lifecycle[Publisher[PublishCommand] => Publisher[PublishCommand]] = timeout match {
    case finiteTimeout: FiniteDuration =>
      ExecutorLifecycles.singleThreadScheduledExecutor.map(ec => new TimeoutPublisher(_, finiteTimeout)(ec))
    case infinite => NoOpLifecycle(identity)
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
        queue.queueName.value,
        queue.isDurable,
        queue.isExclusive,
        queue.shouldAutoDelete,
        queue.arguments.asJava)
    }
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] =
    Lifecycle.using(channelFactory)(channel => thunk(ChannelAmqpOps(channel)))
}

