package com.itv.bucky

import com.itv.bucky.pattern.requeue.RequeuePolicy
import org.scalatest.Assertion

import scala.language.higherKinds


case class TestFixture[F[_]](publisher: Publisher[F, PublishCommand], routingKey: RoutingKey, exchangeName: ExchangeName, queueName: QueueName, amqpClient: BaseAmqpClient, dlqHandler: Option[StubConsumeHandler[F, Delivery]] = None) {

  def requeueQueueName = QueueName(s"${queueName.value}.requeue")

  def publish(body: Payload, properties: MessageProperties = MessageProperties.persistentBasic): F[Unit] = publisher(
    PublishCommand(exchangeName, RoutingKey(queueName.value), properties, body))
}


trait EffectVerification[F[_]] {
  def verifySuccess(f: F[Unit]): Assertion
}

trait PublisherBaseTest[F[_]] extends EffectVerification[F] {
  def withPublisher(testQueueName: QueueName = Any.queue(),
                    requeueStrategy: RequeueStrategy[F] = NoneHandler,
                    shouldDeclare: Boolean = true)(f: TestFixture[F] => Unit): Unit

}


trait PublisherConsumerBaseTest[F[_]] extends EffectVerification[F] {
  def withPublisherAndConsumer(queueName: QueueName = Any.queue(),
                               requeueStrategy: RequeueStrategy[F])(f: TestFixture[F] => Unit): Unit
}


sealed trait RequeueStrategy[+F[_]]

case object NoneHandler extends RequeueStrategy[Nothing]

case class NoneRequeue[F[_]](handler: Handler[F, Delivery]) extends RequeueStrategy[F]

case class SimpleRequeue[F[_]](handler: Handler[F, Delivery]) extends RequeueStrategy[F]

case class RawRequeue[F[_]](handler: RequeueHandler[F, Delivery], requeuePolicy: RequeuePolicy) extends RequeueStrategy[F]

case class TypeRequeue[F[_], T](handler: RequeueHandler[F, T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T])
  extends RequeueStrategy[F]
