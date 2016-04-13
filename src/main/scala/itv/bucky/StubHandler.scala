package itv.bucky


import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class StubHandler[T] extends Handler[T] {

  val receivedMessages = ListBuffer[T]()

  var nextResponse: Future[ConsumeAction] = Future.successful(Ack)

  override def apply(blob: T): Future[ConsumeAction] = {
    receivedMessages += blob
    nextResponse
  }
}