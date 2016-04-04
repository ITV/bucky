package itv

import scala.concurrent.Future

package object bucky {

  type Publisher[-T] = T => Future[Unit]

  type Handler[-T] = T => Future[ConsumeAction]
}
