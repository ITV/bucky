package itv.bucky.decl

import com.typesafe.scalalogging.StrictLogging
import itv.bucky.AmqpClient

import com.itv.lifecycle.VanillaLifecycle

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.{Success, Failure}

case class DeclarationLifecycle(declarations: Iterable[Declaration], client: AmqpClient, timeout: FiniteDuration = 5.seconds) extends VanillaLifecycle[Unit] with StrictLogging {

  override def start(): Unit = {
    logger.info(s"Applying the following declarations: $declarations")

    client.performOps(Declaration.runAll(declarations)) match {
      case Success(_) => ()
      case Failure(reason) => throw new IllegalStateException("Unable to apply all declarations", reason)
    }
  }

  override def shutdown(instance: Unit): Unit = ()

}
