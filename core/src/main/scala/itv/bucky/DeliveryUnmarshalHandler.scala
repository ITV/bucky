package itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

class DeliveryUnmarshalHandler[T, S](unmarshaller: DeliveryUnmarshaller[T])(handler: T => Future[S], deserializationFailureAction: S)
                                    (implicit ec: ExecutionContext) extends (Delivery => Future[S]) with StrictLogging {
  override def apply(delivery: Delivery): Future[S] =
    Future(unmarshaller.unmarshal(delivery)).flatMap {
      case UnmarshalResult.Success(message) => handler(message)
      case UnmarshalResult.Failure(reason, throwable) =>
        logger.error(s"Cannot deserialize: ${delivery.body} because: '$reason' (will $deserializationFailureAction)")
        Future.successful(deserializationFailureAction)
    }
}
