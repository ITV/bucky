package itv.bucky.pattern

import itv.bucky._
import itv.bucky.decl._
import itv.contentdelivery.lifecycle.Lifecycle

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import itv.bucky.Unmarshaller._

package object requeue {

  case class RequeuePolicy(maximumProcessAttempts: Int, requeueAfter: FiniteDuration)

  def requeueDeclarations(queueName: QueueName): Iterable[Declaration] = {
    val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
    val requeueQueueName: QueueName = QueueName(s"${queueName.value}.requeue")
    val dlxExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.dlx")
    val redeliverExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.redeliver")
    val requeueExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.requeue")

    List(
      Queue(queueName).deadLetterExchange(dlxExchangeName),
      Queue(deadLetterQueueName),
      Queue(requeueQueueName).deadLetterExchange(redeliverExchangeName),
      Exchange(dlxExchangeName).binding(RoutingKey(queueName.value) -> deadLetterQueueName),
      Exchange(requeueExchangeName).binding(RoutingKey(queueName.value) -> requeueQueueName),
      Exchange(redeliverExchangeName).binding(RoutingKey(queueName.value) -> queueName)
    )
  }

  implicit class RequeueOps(val amqpClient: AmqpClient) {

    def requeueHandlerOf[T](queueName: QueueName,
                            handler: RequeueHandler[T],
                            requeuePolicy: RequeuePolicy,
                            unmarshaller: PayloadUnmarshaller[T],
                            onFailure: RequeueConsumeAction = Requeue,
                            unmarshalFailureAction: RequeueConsumeAction = DeadLetter)
                           (implicit ec: ExecutionContext): Lifecycle[Unit] = {
     requeueDeliveryHandlerOf(queueName, handler, requeuePolicy, toDeliveryUnmarshaller(unmarshaller), onFailure, unmarshalFailureAction)
    }

    def requeueDeliveryHandlerOf[T](
                                   queueName: QueueName,
                                   handler: RequeueHandler[T],
                                   requeuePolicy: RequeuePolicy,
                                   unmarshaller: DeliveryUnmarshaller[T],
                                   onFailure: RequeueConsumeAction = Requeue,
                                   unmarshalFailureAction: RequeueConsumeAction = DeadLetter)
                                   (implicit ec: ExecutionContext): Lifecycle[Unit] = {
      val deserializeHandler = new DeliveryUnmarshalHandler[T, RequeueConsumeAction](unmarshaller)(handler, unmarshalFailureAction)
      requeueOf(queueName, deserializeHandler, requeuePolicy)
    }

    def requeueOf(queueName: QueueName,
                  handler: RequeueHandler[Delivery],
                  requeuePolicy: RequeuePolicy,
                  onFailure: RequeueConsumeAction = Requeue)
                 (implicit ec: ExecutionContext): Lifecycle[Unit] = {
      val requeueExchange = ExchangeName(s"${queueName.value}.requeue")
      for {
        requeuePublish <- amqpClient.publisher()
        consumer <- amqpClient.consumer(queueName, RequeueTransformer(requeuePublish, requeueExchange, requeuePolicy, onFailure)(handler))
      } yield consumer
    }

  }

}
