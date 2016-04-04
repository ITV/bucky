package itv.bucky

import itv.utils.Blob

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class StubHandler extends Handler[Blob] {

  val receivedMessages = ListBuffer[Blob]()

  var nextResponse: Future[ConsumeAction] = Future.successful(Ack)

  override def apply(blob: Blob): Future[ConsumeAction] = {
    receivedMessages += blob
    nextResponse
  }
}