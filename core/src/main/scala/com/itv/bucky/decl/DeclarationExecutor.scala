package com.itv.bucky.decl

import com.itv.bucky.AmqpClient
import com.itv.bucky.Monad.Id
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success}
import scala.language.higherKinds


object DeclarationExecutor extends StrictLogging{
  def apply[M[_]](declarations: Iterable[Declaration], client: AmqpClient[M], timeout: FiniteDuration = 5.seconds): Id[Unit] = {
    logger.info(s"Applying the following declarations: $declarations")

    client.performOps(Declaration.runAll(declarations)) match {
      case Success(_) => ()
      case Failure(reason) => throw new IllegalStateException("Unable to apply all declarations", reason)
    }
  }
}