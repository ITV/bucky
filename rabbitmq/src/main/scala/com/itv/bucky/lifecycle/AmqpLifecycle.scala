package com.itv.bucky.lifecycle

import com.itv.bucky._
import com.itv.bucky.decl.{Declaration, DeclarationExecutor}
import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}
import com.rabbitmq.client.{Channel, Connection}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.concurrent.duration._

case class AmqpClientLifecycle(config: AmqpClientConfig)(implicit executionContext: ExecutionContext) extends Lifecycle[AmqpClient[Lifecycle, Future, Throwable, Unit]] with StrictLogging {

  override type ServiceInstance = Connection

  override def start(): Connection = IdConnection(config)

  override def shutdown(instance: Connection): Unit = IdConnection.close(instance)

  override def unwrap(instance: Connection): AmqpClient[Lifecycle, Future, Throwable, Unit] = new LifecycleRawAmqpClient(AmqpChannelLifecycle(instance))
}


case class AmqpChannelLifecycle(connection: Connection) extends VanillaLifecycle[Channel] with StrictLogging {
  override def start(): Channel = IdChannel(connection)

  override def shutdown(channel: Channel): Unit = IdChannel.close(channel)
}





case class DeclarationLifecycle[F[_], E](declarations: Iterable[Declaration], client: AmqpClient[Lifecycle, F, E, Unit], timeout: FiniteDuration = 5.seconds) extends VanillaLifecycle[Unit] {

  def start(): Unit = DeclarationExecutor(declarations, client, timeout)

  override def shutdown(instance: Unit): Unit = ()

}