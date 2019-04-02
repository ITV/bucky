package com.itv.bucky.wiring
import _root_.fs2.Stream
import cats.effect.{ContextShift, IO}
import com.itv.bucky.Monad.Id
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.PublishCommandBuilder.publishCommandBuilder
import com.itv.bucky._
import com.itv.bucky.decl.DeclarationExecutor
import com.itv.lifecycle.Lifecycle

import scala.concurrent.Future
import scala.language.{higherKinds, implicitConversions}

final class WiringOps[T](val wiring: Wiring[T]) extends AnyVal {

  def fs2StubConsumeHandler(client: AmqpClient[Id, IO, Throwable, Stream[IO, Unit]])(
      implicit m: Monad[IO],
      cs: ContextShift[IO]): Stream[IO, StubConsumeHandler[IO, Delivery]] = {
    DeclarationExecutor(wiring.consumerDeclarations, client)
    val stub = new StubConsumeHandler[IO, Delivery]()
    Stream
      .bracket(client.consumer(wiring.queueName, stub).compile.drain.start(cs))(_.cancel)
      .map(_ => stub)
  }

  def lifecycleStubConsumeHandler(client: AmqpClient[Lifecycle, Future, Throwable, Unit])(
      implicit f: Monad[Future]): Lifecycle[StubConsumeHandler[Future, Delivery]] = {
    DeclarationExecutor(wiring.consumerDeclarations, client)
    val stub = new StubConsumeHandler[Future, Delivery]()(f)
    client.monad.map(client.consumer(wiring.queueName, stub))(_ => stub)
  }

  def stringPublisher[B[_], F[_], E, C](client: AmqpClient[B, F, E, C]): B[Publisher[F, String]] = {
    wiring.getLogger.info(
      s"Creating string publisher: " +
        s"exchange=${wiring.exchangeName.value} " +
        s"routingKey=${wiring.routingKey.value} " +
        s"queue=${wiring.queueName.value} " +
        s"type=${wiring.exchangeType.value} " +
        s"requeuePolicy=${wiring.requeuePolicy}")
    DeclarationExecutor(wiring.publisherDeclarations, client)
    client.publisherOf(stringPublisherBuilder)
  }

  def stringPublisherBuilder: PublishCommandBuilder.Builder[String] =
    publishCommandBuilder(StringPayloadMarshaller)
      .using(wiring.exchangeName)
      .using(wiring.routingKey)
}

object WiringOps extends WiringTestOps

trait WiringTestOps {
  implicit def toWiringOps[T](wiring: Wiring[T]): WiringOps[T] = new WiringOps(wiring)
}
