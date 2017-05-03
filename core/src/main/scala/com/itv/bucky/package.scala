package com.itv

import java.lang.management.ManagementFactory
import java.util.Date

import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}

import scala.concurrent.{ExecutionContext, Future}

package object bucky {
  type Publisher[-T] = T => Future[Unit]
  type Handler[-T] = T => Future[ConsumeAction]
  type RequeueHandler[-T] = T => Future[RequeueConsumeAction]

  object Handler {
    def apply[T](f: T => Future[ConsumeAction]): Handler[T] =
      new Handler[T] {
        override def apply(message: T): Future[ConsumeAction] =
          f(message)
      }
  }

  object RequeueHandler {
    def apply[T](f: T => Future[RequeueConsumeAction]): RequeueHandler[T] =
      new RequeueHandler[T] {
        override def apply(message: T): Future[RequeueConsumeAction] =
          f(message)
      }
  }

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
    def create(queueName: QueueName): ConsumerTag = ConsumerTag(s"${ManagementFactory.getRuntimeMXBean.getName}-${queueName.value}")
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


  import scala.language.higherKinds

  trait Monad[M[_]] {
    def apply[A](a: => A): M[A]
    def map[A, B](m: M[A])(f: A => B): M[B]
    def flatMap[A, B](m: M[A])(f: A => M[B]): M[B]
  }

  object Monad {

    type Id[A] = A

    implicit val idMonad = new Monad[Id] {
      override def apply[A](a: => A): Id[A] = a

      override def map[A, B](m: Id[A])(f: (A) => B): Id[B] = f(m)

      override def flatMap[A, B](m: Id[A])(f: (A) => Id[B]): Id[B] = f(m)
    }
  }


  implicit val lifecycleMonad = new Monad[Lifecycle] {
    override def apply[A](a: => A): Lifecycle[A] = NoOpLifecycle(a)

    override def map[A, B](m: Lifecycle[A])(f: (A) => B): Lifecycle[B] = m.map(f)

    override def flatMap[A, B](m: Lifecycle[A])(f: (A) => Lifecycle[B]): Lifecycle[B] = m.flatMap(f)
  }

}
