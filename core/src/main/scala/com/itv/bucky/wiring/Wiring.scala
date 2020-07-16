package com.itv.bucky.wiring

import cats.effect.{Resource, Sync}
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.{AmqpClient, ExchangeName, PayloadMarshaller, PayloadUnmarshaller, Publisher, QueueName, RoutingKey}
import com.typesafe.scalalogging.StrictLogging
import com.itv.bucky._

import scala.concurrent.duration._
import cats.implicits._
import cats.effect.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, RequeueConsumeAction}
import com.itv.bucky.publish.PublishCommandBuilder

import scala.language.higherKinds

final case class WiringName(value: String) extends AnyVal

class Wiring[T](
    name: WiringName,
    setExchangeName: Option[ExchangeName] = None,
    setRoutingKey: Option[RoutingKey] = None,
    setQueueName: Option[QueueName] = None,
    setExchangeType: Option[ExchangeType] = None,
    setRequeuePolicy: Option[RequeuePolicy] = None,
    setPrefetchCount: Option[Int] = None,
    setDeadLetterExchangeType: Option[ExchangeType] = None
)(implicit
  val marshaller: PayloadMarshaller[T],
  val unmarshaller: PayloadUnmarshaller[T])
    extends StrictLogging {

  def exchangeName: ExchangeName =
    setExchangeName.getOrElse(ExchangeName(s"bucky.exchange.${name.value}"))
  def routingKey: RoutingKey =
    setRoutingKey.getOrElse(RoutingKey(s"bucky.route.${name.value}"))
  def queueName: QueueName =
    setQueueName.getOrElse(QueueName(s"bucky.queue.${name.value}"))
  def exchangeType: ExchangeType =
    setExchangeType.getOrElse(Topic)
  def requeuePolicy: RequeuePolicy =
    setRequeuePolicy.getOrElse(RequeuePolicy(maximumProcessAttempts = 10, 1.seconds))
  def prefetchCount: Int =
    setPrefetchCount.getOrElse(1)
  lazy val dlxType: ExchangeType =
    setDeadLetterExchangeType.getOrElse(Fanout)
  def dlxRoutingKey: RoutingKey =
    if (dlxType == Fanout) RoutingKey("-") else routingKey

  def exchange: Exchange =
    Exchange(exchangeName, exchangeType = exchangeType)
  def exchangeWithBinding: Exchange =
    exchange.binding(routingKey -> queueName)
  def publisherDeclarations: List[Declaration] =
    List(exchange)
  def consumerDeclarations: List[Declaration] =
    List(exchangeWithBinding) ++ requeue.requeueDeclarations(queueName, dlxType = dlxType, dlxRoutingKey)
  def allDeclarations: List[Declaration] =
    (publisherDeclarations ++ consumerDeclarations).distinct

  def publisher[F[_]](client: AmqpClient[F], timeout: FiniteDuration = 10.seconds)(implicit F: Sync[F]): F[Publisher[F, T]] =
    for {
      _ <- F.delay(
        logger.info(
          s"Creating publisher: " +
            s"exchange=${exchangeName.value} " +
            s"routingKey=${routingKey.value} " +
            s"queue=${queueName.value} " +
            s"type=${exchangeType.value} " +
            s"requeuePolicy=$requeuePolicy"))
      _ <- client.declare(publisherDeclarations)
    } yield client.publisherOf(publisherBuilder)

  def publisherWithHeaders[F[_]](client: AmqpClient[F])(implicit F: Sync[F]): F[PublisherWithHeaders[F, T]] =
    for {
      _ <- F.delay {
        logger.info(
          s"Creating publisher with headers: " +
            s"exchange=${exchangeName.value} " +
            s"routingKey=${routingKey.value} " +
            s"queue=${queueName.value} " +
            s"type=${exchangeType.value} " +
            s"requeuePolicy=$requeuePolicy")
      }
      _ <- client.declare(publisherDeclarations)
    } yield client.publisherWithHeadersOf(publisherBuilder)

  def registerConsumer[F[_]](client: AmqpClient[F])(handleMessage: T => F[ConsumeAction])(implicit F: Sync[F]): Resource[F, Unit] = {
    val runDeclarations =
      for {
        _ <- F.delay {
          logger.info(
            s"Creating consumer: " +
              s"exchange=${exchangeName.value} " +
              s"routingKey=${routingKey.value} " +
              s"queue=${queueName.value} " +
              s"type=${exchangeType.value} " +
              s"requeuePolicy=$requeuePolicy")
        }
        _ <- client.declare(consumerDeclarations)
      } yield ()

    for {
      _ <- Resource.make(runDeclarations)(_ => F.pure(()))
      _ <- client.registerConsumerOf(queueName, handleMessage)
    } yield ()
  }

  def registerRequeueConsumer[F[_]](client: AmqpClient[F])(handleMessage: T => F[RequeueConsumeAction])(implicit F: Sync[F]): Resource[F, Unit] = {
    val runDeclarations =
      for {
        _ <- F.delay {
          logger.info(
            s"Creating consumer: " +
              s"exchange=${exchangeName.value} " +
              s"routingKey=${routingKey.value} " +
              s"queue=${queueName.value} " +
              s"type=${exchangeType.value} " +
              s"requeuePolicy=$requeuePolicy")
        }
        _ <- client.declare(consumerDeclarations)
      } yield ()

    for {
      _ <- Resource.make(runDeclarations)(_ => F.pure(()))
      _ <- client.registerRequeueConsumerOf(queueName, handleMessage, requeuePolicy)(unmarshaller, F)
    } yield ()
  }

  def registerRequeueConsumer[F[_]](client: AmqpClient[F], onRequeueExpiryAction: T => F[ConsumeAction])(handleMessage: T => F[RequeueConsumeAction])(
      implicit F: Sync[F]): Resource[F, Unit] = {
    val runDeclarations =
      for {
        _ <- F.delay {
          logger.info(
            s"Creating consumer: " +
              s"exchange=${exchangeName.value} " +
              s"routingKey=${routingKey.value} " +
              s"queue=${queueName.value} " +
              s"type=${exchangeType.value} " +
              s"requeuePolicy=$requeuePolicy")
        }
        _ <- client.declare(consumerDeclarations)
      } yield ()

    for {
      _ <- Resource.make(runDeclarations)(_ => F.pure(()))
      _ <- client.registerRequeueConsumerOf(queueName = queueName,
                                            handler = handleMessage,
                                            requeuePolicy = requeuePolicy,
                                            onRequeueExpiryAction = onRequeueExpiryAction)(unmarshaller, F)
    } yield ()
  }

  def publisherBuilder: PublishCommandBuilder.Builder[T] =
    publishCommandBuilder(marshaller)
      .using(exchangeName)
      .using(routingKey)

  private[wiring] def getLogger = logger
}
