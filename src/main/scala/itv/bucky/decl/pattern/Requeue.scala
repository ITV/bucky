package itv.bucky.decl.pattern

import itv.bucky._
import itv.bucky.decl._

import scala.concurrent.duration.FiniteDuration

object Pattern {

  object Requeue {
    def apply(queueName: QueueName, retryAfter: FiniteDuration): Iterable[Declaration] = {
      val deadLetterQueueName: QueueName = QueueName(s"${queueName.value}.dlq")
      val requeueQueueName: QueueName = QueueName(s"${queueName.value}.requeue")
      val dlxExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.dlx")
      val redeliverExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.redeliver")
      val requeueExchangeName: ExchangeName = ExchangeName(s"${queueName.value}.requeue")

      List(
        Queue(queueName).deadLetterExchange(dlxExchangeName),
        Queue(deadLetterQueueName),
        Queue(requeueQueueName).deadLetterExchange(redeliverExchangeName).messageTTL(retryAfter),
        Exchange(dlxExchangeName).binding(RoutingKey(queueName.value) -> deadLetterQueueName),
        Exchange(requeueExchangeName).binding(RoutingKey(queueName.value) -> requeueQueueName),
        Exchange(redeliverExchangeName).binding(RoutingKey(queueName.value) -> queueName)
      )
    }
  }

}
