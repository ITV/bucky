package itv.bucky.decl

import itv.bucky.AmqpClient
import itv.contentdelivery.lifecycle.VanillaLifecycle

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

case class DeclarationLifecycle(declarations: Iterable[Declaration], client: AmqpClient, timeout: FiniteDuration = 5.seconds)(implicit ec: ExecutionContext) extends VanillaLifecycle[Unit] {

  override def start(): Unit = {
    val apply = Declaration.applyAll(declarations, client)
    Await.result(apply, timeout)
  }

  override def shutdown(instance: Unit): Unit = ()

}
