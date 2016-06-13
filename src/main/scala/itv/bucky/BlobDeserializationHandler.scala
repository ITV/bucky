package itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

class BlobDeserializationHandler[T, S](deserializer: BlobDeserializer[T])(handler: T => Future[S], deserializationFailureAction: S)
                                      (implicit ec: ExecutionContext) extends (Delivery => Future[S]) with StrictLogging {
  override def apply(delivery: Delivery): Future[S] =
    Future(deserializer(delivery.body)).flatMap {
      case DeserializerResult.Success(message) => handler(message)
      case DeserializerResult.Failure(reason) =>
        logger.error(s"Cannot deserialize: ${delivery.body} because: '$reason' (will $deserializationFailureAction)")
        Future.successful(deserializationFailureAction)
    }.recoverWith { case error: Throwable =>
      logger.error(s"Cannot deserialize: ${delivery.body} because: '${error.getMessage}' (will $deserializationFailureAction)", error)
      Future.successful(deserializationFailureAction)
    }
}
