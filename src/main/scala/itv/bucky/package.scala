package itv

import itv.utils.Blob

import scala.concurrent.Future

package object bucky {

  sealed trait DeserializerResult[T]

  object DeserializerResult {

    case class Success[T](value: T) extends DeserializerResult[T]
    case class Failure[T](reason: String) extends DeserializerResult[T]

    implicit class SuccessConverter[T](val value: T) {
      def success: DeserializerResult[T] = Success(value)
    }

    implicit class FailureConverter[T](val reason: String) {
      def failure: DeserializerResult[T] = Failure(reason)
    }

  }

  trait BlobDeserializer[T] extends (Blob => DeserializerResult[T])

  trait PublishCommandSerializer[T] {
    def toPublishCommand(t: T): PublishCommand
  }

  type Publisher[-T] = T => Future[Unit]

  type Handler[-T] = T => Future[ConsumeAction]

}
