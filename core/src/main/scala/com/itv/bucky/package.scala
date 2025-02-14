package com.itv

import java.nio.charset.{Charset, StandardCharsets}
import cats.effect.{Resource, Sync}
import cats.implicits.toFunctorOps
import cats.{Applicative, ApplicativeError}
import com.itv.bucky.Unmarshaller.toDeliveryUnmarshaller
import com.itv.bucky.consume._
import com.itv.bucky.decl.Declaration
import com.itv.bucky.pattern.requeue.{RequeueOps, RequeuePolicy}
import com.itv.bucky.publish.PublishCommandBuilder

import scala.concurrent.duration._

package object bucky {

  type Publisher[F[_], -T]            = T => F[Unit]
  type PublisherWithHeaders[F[_], -T] = (T, Map[String, AnyRef]) => F[Unit]
  type Handler[F[_], -T]              = T => F[ConsumeAction]
  type RequeueHandler[F[_], -T]       = T => F[RequeueConsumeAction]
  type Bindings                       = PartialFunction[RoutingKey, QueueName]
  type PayloadUnmarshaller[T]         = Unmarshaller[Payload, T]
  type DeliveryUnmarshaller[T]        = Unmarshaller[Delivery, T]
  type UnmarshalResult[T]             = Either[Throwable, T]

  case class RoutingKey(value: String)
  case class ExchangeName(value: String)
  case class QueueName(value: String)

  def publishCommandBuilder[T](marshaller: PayloadMarshaller[T]): PublishCommandBuilder.NothingSet[T] =
    PublishCommandBuilder.publishCommandBuilder[T](marshaller)

  implicit class ConsumerSugar[F[_]](amqpClient: AmqpClient[F])(implicit val F: Sync[F]) {

    def registerConsumerOf[T](
        queueName: QueueName,
        handler: Handler[F, T],
        exceptionalAction: ConsumeAction = DeadLetter,
        prefetchCount: Int = defaultPreFetchCount
    )(implicit payloadUnmarshaller: PayloadUnmarshaller[T], ae: ApplicativeError[F, Throwable]): Resource[F, Unit] =
      amqpClient.registerConsumer(
        queueName,
        (delivery: Delivery) =>
          payloadUnmarshaller.unmarshal(delivery.body) match {
            case Right(value) =>
              handler.apply(value)
            case Left(e) =>
              ae.raiseError(e)
          },
        exceptionalAction,
        prefetchCount
      )

    def registerRequeueConsumerOf[T](
        queueName: QueueName,
        handler: RequeueHandler[F, T],
        requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
        onHandlerException: RequeueConsumeAction = Requeue,
        unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
        onRequeueExpiryAction: T => F[ConsumeAction] = (_: T) => F.point[ConsumeAction](DeadLetter),
        prefetchCount: Int = defaultPreFetchCount
    )(implicit unmarshaller: PayloadUnmarshaller[T], F: Sync[F]): Resource[F, Unit] =
      new RequeueOps(amqpClient).requeueDeliveryHandlerOf[T](
        queueName = queueName,
        handler = handler,
        requeuePolicy = requeuePolicy,
        unmarshaller = toDeliveryUnmarshaller(unmarshaller),
        onHandlerException = onHandlerException,
        unmarshalFailureAction = unmarshalFailureAction,
        onRequeueExpiryAction = onRequeueExpiryAction,
        prefetchCount = prefetchCount
      )

    def registerDeliveryRequeueConsumerOf[T](
        queueName: QueueName,
        handler: RequeueHandler[F, T],
        requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
        onHandlerException: RequeueConsumeAction = Requeue,
        unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
        onRequeueExpiryAction: T => F[ConsumeAction] = (_: T) => F.point[ConsumeAction](DeadLetter),
        prefetchCount: Int = defaultPreFetchCount
    )(implicit unmarshaller: DeliveryUnmarshaller[T], F: Sync[F]): Resource[F, Unit] =
      new RequeueOps(amqpClient).requeueDeliveryHandlerOf[T](
        queueName = queueName,
        handler = handler,
        requeuePolicy = requeuePolicy,
        unmarshaller = unmarshaller,
        onHandlerException = onHandlerException,
        unmarshalFailureAction = unmarshalFailureAction,
        onRequeueExpiryAction = onRequeueExpiryAction,
        prefetchCount = prefetchCount
      )

    def registerRequeueConsumer(
        queueName: QueueName,
        handler: RequeueHandler[F, Delivery],
        requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 3.minutes),
        onHandlerException: RequeueConsumeAction = Requeue,
        prefetchCount: Int = defaultPreFetchCount
    )(implicit F: Sync[F]): Resource[F, Unit] =
      new RequeueOps(amqpClient).requeueOf(queueName, handler, requeuePolicy, onHandlerException, prefetchCount = prefetchCount)

  }

  implicit class PublisherSugar[F[_]: Applicative](amqpClient: AmqpClient[F]) {

    def publisherOf[T](mandatory: Boolean)(implicit publishCommandBuilder: PublishCommandBuilder[T]): F[Publisher[F, T]] =
      amqpClient.publisher(mandatory).map { basePublisher => (value: T) =>
        {
          val command = publishCommandBuilder.toPublishCommand(value)
          basePublisher.apply(command)
        }
      }

    def publisherOf[T](implicit publishCommandBuilder: PublishCommandBuilder[T]): F[Publisher[F, T]] =
      publisherOf(mandatory = false)

    def publisherOf[T](exchangeName: ExchangeName, routingKey: RoutingKey, mandatory: Boolean)(implicit
        marshaller: PayloadMarshaller[T]
    ): F[Publisher[F, T]] = {
      val pcb =
        PublishCommandBuilder
          .publishCommandBuilder(marshaller)
          .using(exchangeName)
          .using(routingKey)
      publisherOf[T](mandatory)(pcb)
    }

    def publisherOf[T](exchangeName: ExchangeName, routingKey: RoutingKey)(implicit
        marshaller: PayloadMarshaller[T]
    ): F[Publisher[F, T]] =
      publisherOf(exchangeName, routingKey, mandatory = false)

    def publisherWithHeadersOf[T](exchangeName: ExchangeName, routingKey: RoutingKey, mandatory: Boolean)(implicit
        F: Sync[F],
        marshaller: PayloadMarshaller[T]
    ): F[PublisherWithHeaders[F, T]] = {
      val pcb =
        PublishCommandBuilder
          .publishCommandBuilder(marshaller)
          .using(exchangeName)
          .using(routingKey)
      publisherWithHeadersOf[T](pcb, mandatory)
    }

    def publisherWithHeadersOf[T](exchangeName: ExchangeName, routingKey: RoutingKey)(implicit
        F: Sync[F],
        marshaller: PayloadMarshaller[T]
    ): F[PublisherWithHeaders[F, T]] =
      publisherWithHeadersOf[T](exchangeName, routingKey, mandatory = false)

    def publisherWithHeadersOf[T](commandBuilder: PublishCommandBuilder[T], mandatory: Boolean)(implicit F: Sync[F]): F[PublisherWithHeaders[F, T]] =
      amqpClient.publisher(mandatory).map { publisher => (message: T, headers: Map[String, AnyRef]) =>
        F.flatMap(F.delay {
          val command = commandBuilder.toPublishCommand(message)

          command.copy(basicProperties = headers.foldLeft(command.basicProperties) { case (props, (headerName, headerValue)) =>
            props.withHeader(headerName -> headerValue)
          })
        })(publisher)
      }

    def publisherWithHeadersOf[T](commandBuilder: PublishCommandBuilder[T])(implicit F: Sync[F]): F[PublisherWithHeaders[F, T]] =
      publisherWithHeadersOf(commandBuilder, mandatory = false)

  }

  implicit class DeclareSugar[F[_]](amqpClient: AmqpClient[F])(implicit a: Applicative[F]) {
    def declareR(declarations: Declaration*): Resource[F, Unit]          = Resource.eval[F, Unit](amqpClient.declare(declarations))
    def declareR(declarations: Iterable[Declaration]): Resource[F, Unit] = Resource.eval[F, Unit](amqpClient.declare(declarations))
  }

  implicit class LoggingSyntax[F[_]](client: AmqpClient[F]) {
    def withLogging(charset: Charset = StandardCharsets.UTF_8)(implicit F: Sync[F]): AmqpClient[F] = LoggingAmqpClient(client, charset)
  }

  val defaultPreFetchCount: Int = 1

}
