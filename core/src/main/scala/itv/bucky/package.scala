package itv

import java.lang.management.ManagementFactory
import java.util.Date

import scala.concurrent.{ExecutionContext, Future}

package object bucky {

  case class PublishCommand(exchange: ExchangeName, routingKey: RoutingKey, basicProperties: AmqpProperties, body: Payload) {
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

  case class AmqpProperties(
                             contentType: Option[String] = Some("application/octet-stream"),
                             contentEncoding: Option[String] = None,
                             headers: Map[String, AnyRef] = Map(),
                             deliveryMode: Option[Int] = Some(2),
                             priority: Option[Int] = Some(0),
                             correlationId: Option[String] = None,
                             replyTo: Option[String] = None,
                             expiration: Option[String] = None,
                             messageId: Option[String] = None,
                             timestamp: Option[Date] = None,
                             `type`: Option[String] = None,
                             userId: Option[String] = None,
                             appId: Option[String] = None,
                             clusterId: Option[String] = None
                           )

  case class Envelope(deliveryTag: Long, redeliver: Boolean, exchangeName: ExchangeName, routingKey: RoutingKey)

  case class Delivery(body: Payload, consumerTag: ConsumerTag, envelope: Envelope, properties: AmqpProperties)

  type Publisher[-T] = T => Future[Unit]
  type Handler[-T] = T => Future[ConsumeAction]
  type RequeueHandler[-T] = T => Future[RequeueConsumeAction]

  type Bindings = PartialFunction[RoutingKey, QueueName]

  def safePerform[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = Future(future).flatMap(identity)

  type PayloadUnmarshaller[T] = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T] = Unmarshaller[Delivery, T]

}
