package itv.bucky.pattern

import itv.bucky.Unmarshaller._
import itv.bucky._
import itv.bucky.decl._
import com.itv.lifecycle.Lifecycle

import scala.concurrent.duration._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

package object requeue {

  case class RequeuePolicy(maximumProcessAttempts: Int, requeueAfter: FiniteDuration)

  def basicRequeueDeclarations(queueName: QueueName, retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
    val dlxExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.dlx")

    requeueDeclarations(queueName, RoutingKey(queueName.value),
      Exchange(dlxExchangeName).binding(RoutingKey(queueName.value) -> deadLetterQueueName), retryAfter)
  }

  def requeueDeclarations(queueName: QueueName, routingKey: RoutingKey): Iterable[Declaration] = {
    requeueDeclarations(queueName, routingKey, Exchange(ExchangeName(s"${queueName.value}.dlx")))
  }

  def requeueDeclarations(queueName: QueueName, routingKey: RoutingKey, deadletterExchange: Exchange, retryAfter: FiniteDuration = 5.minutes): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
    val requeueQueueName: QueueName = QueueName(s"${queueName.value}.requeue")
    val redeliverExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.redeliver")
    val requeueExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.requeue")

    List(
      Queue(queueName).deadLetterExchange(deadletterExchange.name),
      Queue(deadLetterQueueName),
      Queue(requeueQueueName).deadLetterExchange(redeliverExchangeName).messageTTL(retryAfter),
      deadletterExchange.binding(routingKey -> deadLetterQueueName),
      Exchange(requeueExchangeName).binding(routingKey -> requeueQueueName),
      Exchange(redeliverExchangeName).binding(routingKey -> queueName)
    )
  }

  implicit class RequeueOps(val amqpClient: AmqpClient) {

    def requeueHandlerOf[T](queueName: QueueName,
                            handler: RequeueHandler[T],
                            requeuePolicy: RequeuePolicy,
                            unmarshaller: PayloadUnmarshaller[T],
                            onFailure: RequeueConsumeAction = Requeue,
                            unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                            prefetchCount: Int = 0)
                           (implicit ec: ExecutionContext): Lifecycle[Unit] = {
      requeueDeliveryHandlerOf(queueName, handler, requeuePolicy, toDeliveryUnmarshaller(unmarshaller), onFailure, unmarshalFailureAction, prefetchCount)
    }

    def requeueDeliveryHandlerOf[T](
                                     queueName: QueueName,
                                     handler: RequeueHandler[T],
                                     requeuePolicy: RequeuePolicy,
                                     unmarshaller: DeliveryUnmarshaller[T],
                                     onFailure: RequeueConsumeAction = Requeue,
                                     unmarshalFailureAction: RequeueConsumeAction = DeadLetter,
                                     prefetchCount: Int = 0)
                                   (implicit ec: ExecutionContext): Lifecycle[Unit] = {
      val deserializeHandler = new DeliveryUnmarshalHandler[T, RequeueConsumeAction](unmarshaller)(handler, unmarshalFailureAction)
      requeueOf(queueName, deserializeHandler, requeuePolicy, prefetchCount = prefetchCount)
    }

    def requeueOf(queueName: QueueName,
                  handler: RequeueHandler[Delivery],
                  requeuePolicy: RequeuePolicy,
                  onFailure: RequeueConsumeAction = Requeue,
                  prefetchCount: Int = 0)
                 (implicit ec: ExecutionContext): Lifecycle[Unit] = {
      val requeueExchange = ExchangeName(s"${queueName.value}.requeue")
      for {
        requeuePublish <- amqpClient.publisher()
        consumer <- amqpClient.consumer(queueName, RequeueTransformer(requeuePublish, requeueExchange, requeuePolicy, onFailure)(handler), prefetchCount = prefetchCount)
      } yield consumer
    }

  }

}
