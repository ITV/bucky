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

  def requeueDeclarations(queueName: QueueName,
                          retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val dlxExchangeName: ExchangeName       = ExchangeName(s"${queueName.value}.dlx.v2")
    val deadLetterQueueName: QueueName      = QueueName(s"${queueName.value}.dlq")
    val requeueQueueName: QueueName         = QueueName(s"${queueName.value}.requeue")
    val redeliverExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.redeliver.v2")
    val requeueExchangeName: ExchangeName   = ExchangeName(s"${queueName.value}.requeue.v2")

    List(
      Queue(queueName).deadLetterExchange(dlxExchangeName),
      Queue(deadLetterQueueName),
      Queue(requeueQueueName).deadLetterExchange(redeliverExchangeName).messageTTL(retryAfter),
      Exchange(dlxExchangeName, exchangeType = Fanout).binding(RoutingKey("-") -> deadLetterQueueName),
      Exchange(requeueExchangeName, exchangeType = Fanout).binding(RoutingKey("-") -> requeueQueueName),
      Exchange(redeliverExchangeName, exchangeType = Fanout).binding(RoutingKey("-") -> queueName)
    )
  }

  private[bucky] class RequeueOps[F[_]](val amqpClient: AmqpClient[F])(implicit val F: Sync[F]) {

    def requeueDeliveryHandlerOf[T](queueName: QueueName,
                                    handler: RequeueHandler[F, T],
                                    requeuePolicy: RequeuePolicy,
                                    unmarshaller: DeliveryUnmarshaller[T],
                                    onHandlerException: RequeueConsumeAction = Requeue,
                                    onRequeueExpiryAction: T => F[ConsumeAction] = (_: T) => F.point[ConsumeAction](DeadLetter),
                                    unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                                    prefetchCount: Int = defaultPreFetchCount): Resource[F, Unit] = {

      val deserializeHandler                              = new DeliveryUnmarshalHandler[F, T, RequeueConsumeAction](unmarshaller)(handler, unmarshalFailureAction)
      val deserializeOnRequeueExpiryAction: Delivery => F[ConsumeAction] = new UnmarshalFailureAction[F, T](unmarshaller).apply(onRequeueExpiryAction)
      requeueOf(queueName, deserializeHandler, requeuePolicy, onHandlerException, deserializeOnRequeueExpiryAction, prefetchCount)
    }

    def requeueOf(queueName: QueueName,
                  handler: RequeueHandler[F, Delivery],
                  requeuePolicy: RequeuePolicy,
                  onHandlerException: RequeueConsumeAction = Requeue,
                  onRequeueExpiryAction: Delivery => F[ConsumeAction] = (_: Delivery) => F.point[ConsumeAction](DeadLetter),
                  prefetchCount: Int = defaultPreFetchCount): Resource[F, Unit] = {
      val requeueExchange = ExchangeName(s"${queueName.value}.requeue.v2")
      val requeuePublish  = amqpClient.publisher()
      amqpClient.registerConsumer(queueName,
                                  RequeueTransformer(requeuePublish, requeueExchange, requeuePolicy, onHandlerException, onRequeueExpiryAction)(handler),
                                  prefetchCount = prefetchCount)
    }
  }
}
