package itv.bucky

import com.rabbitmq.client.MessageProperties
import itv.contentdelivery.testutilities.SameThreadExecutionContext

import scala.concurrent.{Promise, Future}
import scala.util.{Random, Try}

object TestUtils {

  def anyPublishCommand() = PublishCommand(ExchangeName("exchange"), RoutingKey("routing.key"), MessageProperties.MINIMAL_PERSISTENT_BASIC, Payload.from("msg" + Random.nextInt()))

  implicit class FutureOps[T](f: Future[T]) {
    def asTry: Future[Try[T]] = {
      val p = Promise[Try[T]]()
      f.onComplete(p.success)(SameThreadExecutionContext)
      p.future
    }
  }
}
