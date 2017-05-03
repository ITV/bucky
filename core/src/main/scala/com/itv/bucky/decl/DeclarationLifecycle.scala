package com.itv.bucky.decl

import com.itv.bucky.AmqpClient
import com.itv.lifecycle.VanillaLifecycle
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success}
import scala.language.higherKinds

case class DeclarationLifecycle[M[_]](declarations: Iterable[Declaration], client: AmqpClient[M], timeout: FiniteDuration = 5.seconds) extends VanillaLifecycle[Unit] with StrictLogging {

  def start(): Unit = {
    logger.info(s"Applying the following declarations: $declarations")

    client.performOps(Declaration.runAll(declarations)) match {
      case Success(_) => ()
      case Failure(reason) => throw new IllegalStateException("Unable to apply all declarations", reason)
    }
  }

  override def shutdown(instance: Unit): Unit = ()
}
