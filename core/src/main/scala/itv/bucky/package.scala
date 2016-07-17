package itv

import java.lang.management.ManagementFactory
import java.util.Date

import scala.concurrent.{ExecutionContext, Future}

package object bucky {
  type Publisher[-T] = T => Future[Unit]
  type Handler[-T] = T => Future[ConsumeAction]
  type RequeueHandler[-T] = T => Future[RequeueConsumeAction]

  type Bindings = PartialFunction[RoutingKey, QueueName]

  type PayloadUnmarshaller[T] = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T] = Unmarshaller[Delivery, T]

  case class PublishCommand(exchange: ExchangeName, routingKey: RoutingKey, basicProperties: MessageProperties, body: Payload) {
    def description = s"${exchange.value}:${routingKey.value} $body"
  }

  case class RoutingKey(value: String)

  case class ExchangeName(value: String)

  case class QueueName(value: String)

  sealed trait ConsumeAction

  sealed trait RequeueConsumeAction

  case object Ack extends ConsumeAction with RequeueConsumeAction

  case object DeadLetter extends ConsumeAction with RequeueConsumeAction

  case object RequeueImmediately extends ConsumeAction with RequeueConsumeAction

  case object Requeue extends RequeueConsumeAction

  case class ConsumerTag(value: String)

  object ConsumerTag {
    val pidAndHost: ConsumerTag = ConsumerTag(ManagementFactory.getRuntimeMXBean.getName)
  }

  case class Envelope(deliveryTag: Long, redeliver: Boolean, exchangeName: ExchangeName, routingKey: RoutingKey)

  case class Delivery(body: Payload, consumerTag: ConsumerTag, envelope: Envelope, properties: MessageProperties)

  case class MessageProperties(
                                contentType: Option[ContentType],
                                contentEncoding: Option[ContentEncoding],
                                headers: Map[String, AnyRef],
                                deliveryMode: Option[DeliveryMode],
                                priority: Option[Int],
                                correlationId: Option[String],
                                replyTo: Option[String],
                                expiration: Option[String],
                                messageId: Option[String],
                                timestamp: Option[Date],
                                messageType: Option[String],
                                userId: Option[String],
                                appId: Option[String],
                                clusterId: Option[String]
                              ) {
    def withHeader(header: (String, AnyRef)): MessageProperties = this.copy(
      headers = this.headers + header
    )
  }

  case class DeliveryMode(value: Int)

  object DeliveryMode {
    val persistent = DeliveryMode(2)
    val nonPersistent = DeliveryMode(1)
  }

  case class ContentType(value: String)

  object ContentType {
    val octetStream = ContentType("application/octet-stream")
    val textPlain = ContentType("text/plain")
  }

  case class ContentEncoding(value: String)

  object ContentEncoding {
    val utf8 = ContentEncoding("utf-8")
  }

  object MessageProperties {
    val minimalBasic = MessageProperties(None, None, Map(), None, None, None, None, None, None, None, None, None, None, None)

    val minimalPersistentBasic = minimalBasic.copy(
      deliveryMode = Some(DeliveryMode.persistent))

    val basic = minimalBasic.copy(
      contentType = Some(ContentType.octetStream),
      deliveryMode = Some(DeliveryMode.nonPersistent),
      priority = Some(0))

    val persistentBasic = minimalBasic.copy(
      contentType = Some(ContentType.octetStream),
      deliveryMode = Some(DeliveryMode.persistent),
      priority = Some(0))

    val textPlain = minimalBasic.copy(
      contentType = Some(ContentType.textPlain),
      deliveryMode = Some(DeliveryMode.nonPersistent),
      priority = Some(0)
    )

    val persistentTextPlain = minimalBasic.copy(
      contentType = Some(ContentType.textPlain),
      deliveryMode = Some(DeliveryMode.persistent),
      priority = Some(0)
    )
  }

  def safePerform[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = Future(future).flatMap(identity)

}
