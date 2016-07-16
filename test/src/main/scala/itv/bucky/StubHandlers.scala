package itv.bucky

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class StubHandler[T, S](var nextResponse: Future[S], var nextException: Option[Throwable] = None) extends (T => Future[S]) {

  val receivedMessages = ListBuffer[T]()

  override def apply(message: T): Future[S] = {
    receivedMessages += message
    nextException.fold[Future[S]](nextResponse)(throw _)
  }

}

class StubConsumeHandler[T] extends StubHandler[T, ConsumeAction](Future.successful(Ack))

class StubRequeueHandler[T] extends StubHandler[T, RequeueConsumeAction](Future.successful(Ack) )