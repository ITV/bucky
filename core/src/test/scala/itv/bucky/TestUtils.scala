package itv.bucky

import scala.concurrent.{Future, Promise}
import scala.util.{Random, Try}

object TestUtils {

  def anyPublishCommand() = PublishCommand(ExchangeName("exchange"), RoutingKey("routing.key"), MessageProperties.persistentBasic, Payload.from("msg" + Random.nextInt()))

  implicit class FutureOps[T](f: Future[T]) {
    def asTry: Future[Try[T]] = {
      val p = Promise[Try[T]]()
      f.onComplete(p.success)(SameThreadExecutionContext)
      p.future
    }
  }
}
