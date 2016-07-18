package itv.bucky.decl

import com.typesafe.scalalogging.StrictLogging
import itv.bucky.AmqpClient

import itv.contentdelivery.lifecycle.VanillaLifecycle

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.{Success, Failure}

case class DeclarationLifecycle(declarations: Iterable[Declaration], client: AmqpClient, timeout: FiniteDuration = 5.seconds)
                               (implicit ec: ExecutionContext) extends VanillaLifecycle[Unit] with StrictLogging {

  override def start(): Unit = {
    logger.info(s"Applying the following declarations: $declarations")
    val apply = Declaration.applyAll(declarations, client)
    apply.onComplete {
      case Success(_) => logger.info("Declarations have been applied successfully!")
      case Failure(exception) => logger.error(s"Failure when applying declarations because ${exception.getMessage}", exception)
    }
    Await.result(apply, timeout)
  }

  override def shutdown(instance: Unit): Unit = ()

}
