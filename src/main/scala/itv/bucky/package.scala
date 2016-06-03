package itv

import java.lang.management.ManagementFactory

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import itv.utils.Blob

import scala.concurrent.Future

package object bucky {

  case class PublishCommand(exchange: Exchange, routingKey: RoutingKey, basicProperties: BasicProperties, body: Blob) {
    def description = s"${exchange.value}:${routingKey.value} $body"
  }

  case class RoutingKey(value: String)
  case class Exchange(value: String)
  case class QueueName(value: String)

  sealed trait ConsumeAction
  case object Ack extends ConsumeAction
  case object DeadLetter extends ConsumeAction
  case object RequeueImmediately extends ConsumeAction

  sealed trait RequeueConsumeAction
  case object Requeue extends RequeueConsumeAction
  case class Consume(action: ConsumeAction) extends RequeueConsumeAction

  case class ConsumerTag(value: String)
  object ConsumerTag {
    val pidAndHost: ConsumerTag = ConsumerTag(ManagementFactory.getRuntimeMXBean.getName)
  }

  sealed trait DeserializerResult[T]
  object DeserializerResult {

    case class Success[T](value: T) extends DeserializerResult[T]
    case class Failure[T](reason: String) extends DeserializerResult[T]

    implicit class SuccessConverter[T](val value: T) {
      def success: DeserializerResult[T] = Success(value)
    }

    implicit class FailureConverter[T](val reason: String) {
      def failure: DeserializerResult[T] = Failure(reason)
    }

  }

  trait BlobDeserializer[T] extends (Blob => DeserializerResult[T])

  trait PublishCommandSerializer[T] {
    def toPublishCommand(t: T): PublishCommand
  }

  case class Delivery(body: Blob, consumerTag: ConsumerTag, envelope: Envelope, properties: BasicProperties)

  type Publisher[-T] = T => Future[Unit]
  type Handler[-T] = T => Future[ConsumeAction]
  type RequeueHandler[-T] = T => Future[RequeueConsumeAction]

  type Bindings = PartialFunction[RoutingKey, QueueName]

}
