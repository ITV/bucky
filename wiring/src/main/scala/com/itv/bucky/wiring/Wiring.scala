package com.itv.bucky.wiring

import com.itv.bucky.PublishCommandBuilder
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue.{RequeuePolicy, _}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.language.higherKinds

final case class WiringName(value: String) extends AnyVal

class Wiring[T](
  name: WiringName,
  setExchangeName: Option[ExchangeName] = None,
  setRoutingKey: Option[RoutingKey] = None,
  setQueueName: Option[QueueName] = None,
  setExchangeType: Option[ExchangeType] = None,
  setRequeuePolicy: Option[RequeuePolicy] = None,
  setPrefetchCount: Option[Int] = None
)(implicit
  val marshaller: PayloadMarshaller[T],
  val unmarshaller: PayloadUnmarshaller[T]
)
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

  def exchange: Exchange =
    Exchange(exchangeName, exchangeType = exchangeType)
  def exchangeWithBinding: Exchange =
    exchange.binding(routingKey -> queueName)
  def publisherDeclarations: List[Declaration] =
    List(exchange)
  def consumerDeclarations: List[Declaration] =
    List(exchangeWithBinding) ++ requeueDeclarations(queueName, routingKey)
  def allDeclarations: List[Declaration] =
    (publisherDeclarations ++ consumerDeclarations).distinct

  def publisher[B[_], F[_], E, C](client: AmqpClient[B, F, E, C], timeout: FiniteDuration = 10.seconds): B[Publisher[F, T]] = {
    logger.info(
      s"Creating publisher: " +
        s"exchange=${ exchangeName.value} " +
        s"routingKey=${ routingKey.value} " +
        s"queue=${ queueName.value} " +
        s"type=${ exchangeType.value} " +
        s"requeuePolicy=$requeuePolicy")
    DeclarationExecutor(publisherDeclarations, client)
    client.publisherOf(publisherBuilder, timeout)
  }

  def publisherWithHeaders[B[_], F[_], E, C](client: AmqpClient[B, F, E, C]): B[PublisherWithHeaders[F, T]] = {
    logger.info(
      s"Creating publisher with headers: " +
        s"exchange=${ exchangeName.value} " +
        s"routingKey=${ routingKey.value} " +
        s"queue=${ queueName.value} " +
        s"type=${ exchangeType.value} " +
        s"requeuePolicy=$requeuePolicy")
    DeclarationExecutor(publisherDeclarations, client)
    client.publisherWithHeadersOf(publisherBuilder)
  }

  def consumer[B[_], F[_], E, C](client: AmqpClient[B, F, E, C])(handleMessage: T => F[RequeueConsumeAction]): B[C] = {
    logger.info(
      s"Creating consumer: " +
        s"exchange=${exchangeName.value} " +
        s"routingKey=${routingKey.value} " +
        s"queue=${queueName.value} " +
        s"type=${exchangeType.value} " +
        s"requeuePolicy=$requeuePolicy")
    DeclarationExecutor(consumerDeclarations, client)
    RequeueOps(client)
      .requeueHandlerOf[T](
      queueName = queueName,
      handler = handleMessage,
      requeuePolicy = requeuePolicy,
      unmarshaller = unmarshaller,
      prefetchCount = prefetchCount
    )
  }

  def publisherBuilder: PublishCommandBuilder.Builder[T] =
    PublishCommandBuilder
      .publishCommandBuilder(marshaller)
      .using(exchangeName)
      .using(routingKey)

  private [wiring] def getLogger = logger
}

