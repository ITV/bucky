package itv

import scala.concurrent.Future

package object bucky {

  trait PublishCommandSerializer[T] {
    def toPublishCommand(t: T): PublishCommand
  }

  type Publisher[-T] = T => Future[Unit]

  type Handler[-T] = T => Future[ConsumeAction]

}
