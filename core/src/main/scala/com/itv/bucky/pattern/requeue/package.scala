package com.itv.bucky.pattern

import cats.effect.{Resource, Sync}
import com.itv.bucky.{AmqpClient, DeliveryUnmarshalHandler, _}
import com.itv.bucky.Unmarshaller._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery, Requeue, RequeueConsumeAction}
import com.itv.bucky.decl._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.higherKinds
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery, Requeue, RequeueConsumeAction}

package object requeue {

  case class RequeuePolicy(maximumProcessAttempts: Int, requeueAfter: FiniteDuration)

  def basicRequeueDeclarations(queueName: QueueName, retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
    val dlxExchangeName: ExchangeName  = ExchangeName(s"${queueName.value}.dlx")
    requeueDeclarations(queueName,
                        RoutingKey(queueName.value),
                        Exchange(dlxExchangeName).binding(RoutingKey(queueName.value) -> deadLetterQueueName),
                        retryAfter)
  }

  def requeueDeclarations(queueName: QueueName, routingKey: RoutingKey): Iterable[Declaration] =
    requeueDeclarations(queueName, routingKey, Exchange(ExchangeName(s"${queueName.value}.dlx")))

  def requeueDeclarations(queueName: QueueName,
                          routingKey: RoutingKey,
                          deadletterExchange: Exchange,
                          retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName      = QueueName(s"${queueName.value}.dlq")
    val requeueQueueName: QueueName         = QueueName(s"${queueName.value}.requeue")
    val redeliverExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.redeliver")
    val requeueExchangeName: ExchangeName   = ExchangeName(s"${queueName.value}.requeue")

    List(
      Queue(queueName).deadLetterExchange(deadletterExchange.name),
      Queue(deadLetterQueueName),
      Queue(requeueQueueName).deadLetterExchange(redeliverExchangeName).messageTTL(retryAfter),
      deadletterExchange.binding(routingKey              -> deadLetterQueueName),
      Exchange(requeueExchangeName).binding(routingKey   -> requeueQueueName),
      Exchange(redeliverExchangeName).binding(routingKey -> queueName)
    )
  }

  private[bucky] class RequeueOps[F[_]](val amqpClient: AmqpClient[F])(implicit val F: Sync[F]) {

    def requeueDeliveryHandlerOf[T](queueName: QueueName,
                                    handler: RequeueHandler[F, T],
                                    requeuePolicy: RequeuePolicy,
                                    unmarshaller: DeliveryUnmarshaller[T],
                                    onFailure: RequeueConsumeAction = Requeue,
                                    onFailureAction: T => F[ConsumeAction] = (_: T) => F.point(DeadLetter),
                                    unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                                    prefetchCount: Int = defaultPreFetchCount): Resource[F, Unit] = {

      val deserializeHandler                              = new DeliveryUnmarshalHandler[F, T, RequeueConsumeAction](unmarshaller)(handler, unmarshalFailureAction)
      val deserializeOnFailureAction: Delivery => F[ConsumeAction] = new UnmarshalFailureAction[F, T](unmarshaller).apply(onFailureAction)
      requeueOf(queueName, deserializeHandler, requeuePolicy, onFailure, deserializeOnFailureAction, prefetchCount)
    }

    def requeueOf(queueName: QueueName,
                  handler: RequeueHandler[F, Delivery],
                  requeuePolicy: RequeuePolicy,
                  onFailure: RequeueConsumeAction = Requeue,
                  onFailureAction: Delivery => F[ConsumeAction] = (_: Delivery) => F.point(DeadLetter),
                  prefetchCount: Int = defaultPreFetchCount): Resource[F, Unit] = {
      val requeueExchange = ExchangeName(s"${queueName.value}.requeue")
      val requeuePublish  = amqpClient.publisher()
      amqpClient.registerConsumer(queueName,
                                  RequeueTransformer(requeuePublish, requeueExchange, requeuePolicy, onFailure, onFailureAction)(handler),
                                  prefetchCount = prefetchCount)
    }
  }
}
