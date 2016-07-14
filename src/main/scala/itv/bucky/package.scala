package itv

import java.lang.management.ManagementFactory

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

import scala.concurrent.{ExecutionContext, Future}

package object bucky {

  case class PublishCommand(exchange: ExchangeName, routingKey: RoutingKey, basicProperties: BasicProperties, body: Payload) {
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

  case class Delivery(body: Payload, consumerTag: ConsumerTag, envelope: Envelope, properties: BasicProperties)

  type Publisher[-T] = T => Future[Unit]
  type Handler[-T] = T => Future[ConsumeAction]
  type RequeueHandler[-T] = T => Future[RequeueConsumeAction]

  type Bindings = PartialFunction[RoutingKey, QueueName]

  def safePerform[T](future : => Future[T])(implicit executionContext: ExecutionContext): Future[T] = Future(future).flatMap(identity)

  type PayloadUnmarshaller[T] = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T] = Unmarshaller[Delivery, T]

}
