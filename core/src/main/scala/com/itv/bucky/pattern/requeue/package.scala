package com.itv.bucky.pattern

import com.itv.bucky.{AmqpClient, ConsumeAction, DeliveryUnmarshalHandler, _}
import com.itv.bucky.Unmarshaller._
import com.itv.bucky.decl._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.higherKinds

package object requeue {

  case class RequeuePolicy(maximumProcessAttempts: Int, requeueAfter: FiniteDuration)

  def basicRequeueDeclarations(queueName: QueueName, retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
    val dlxExchangeName: ExchangeName  = ExchangeName(s"${queueName.value}.dlx")

    requeueDeclarations(queueName,
                        RoutingKey(queueName.value),
                        Exchange(dlxExchangeName, exchangeType = Topic).binding(RoutingKey(queueName.value) -> deadLetterQueueName),
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
      Exchange(requeueExchangeName, exchangeType = Topic).binding(routingKey   -> requeueQueueName),
      Exchange(redeliverExchangeName, exchangeType = Topic).binding(routingKey -> queueName)
    )
  }

  implicit class RequeueOps[B[_], F[_], E, C](val amqpClient: AmqpClient[B, F, E, C]) {

    def requeueHandlerOf[T](queueName: QueueName,
                            handler: RequeueHandler[F, T],
                            requeuePolicy: RequeuePolicy,
                            unmarshaller: PayloadUnmarshaller[T],
                            onFailure: RequeueConsumeAction = Requeue,
                            unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                            prefetchCount: Int = 0): B[C] =
      requeueDeliveryHandlerOf(queueName,
        handler,
        requeuePolicy,
        toDeliveryUnmarshaller(unmarshaller),
        onFailure,
        unmarshalFailureAction = unmarshalFailureAction,
        prefetchCount = prefetchCount
      )

    def requeueHandlerWithFailureActionOf[T](queueName: QueueName,
                                             handler: RequeueHandler[F, T],
                                             requeuePolicy: RequeuePolicy,
                                             unmarshaller: PayloadUnmarshaller[T],
                                             onFailure: RequeueConsumeAction = Requeue,
                                             onFailureAction: T => F[Unit],
                                             unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                                             prefetchCount: Int = 0): B[C] = {
      requeueDeliveryHandlerOf(queueName,
        handler,
        requeuePolicy,
        toDeliveryUnmarshaller(unmarshaller),
        onFailure,
        onFailureAction,
        unmarshalFailureAction,
        prefetchCount
      )
    }

    def requeueDeliveryHandlerOf[T](queueName: QueueName,
                                    handler: RequeueHandler[F, T],
                                    requeuePolicy: RequeuePolicy,
                                    unmarshaller: DeliveryUnmarshaller[T],
                                    onFailure: RequeueConsumeAction = Requeue,
                                    onFailureAction: T => F[Unit] = (_: T) => amqpClient.effectMonad.apply(()),
                                    unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                                    prefetchCount: Int = 0): B[C] = {
      val deserializeHandler =
        new DeliveryUnmarshalHandler[F, T, RequeueConsumeAction](unmarshaller)(handler, unmarshalFailureAction)(
          amqpClient.effectMonad)

      val deserializeOnFailureAction: Delivery => F[Unit] =
        new UnmarshalFailureAction[F, T](unmarshaller)(amqpClient.effectMonad)(onFailureAction)

      requeueOf(queueName, deserializeHandler, requeuePolicy, onFailure, deserializeOnFailureAction, prefetchCount = prefetchCount)
    }

    def requeueOf(queueName: QueueName,
                  handler: RequeueHandler[F, Delivery],
                  requeuePolicy: RequeuePolicy,
                  onFailure: RequeueConsumeAction = Requeue,
                  onFailureAction: Delivery => F[Unit] = (_: Delivery) => amqpClient.effectMonad.apply(()),
                  prefetchCount: Int = 0): B[C] = {
      val requeueExchange = ExchangeName(s"${queueName.value}.requeue")
      amqpClient.monad.flatMap(amqpClient.publisher()) { requeuePublish =>
        amqpClient.consumer(queueName,
          RequeueTransformer(requeuePublish, requeueExchange, requeuePolicy, onFailure, onFailureAction)(handler)(
            amqpClient.effectMonad),
          prefetchCount = prefetchCount)
      }
    }
  }
}
