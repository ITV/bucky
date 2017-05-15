package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Assertion, FunSuite}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.Success
import org.scalatest.Matchers._


case class TestFixture[F[_]](publisher: Publisher[F, PublishCommand], routingKey: RoutingKey, exchangeName: ExchangeName, queueName: QueueName, amqpClient: BaseAmqpClient, dlqHandler: Option[StubConsumeHandler[F, Delivery]] = None) {

  def requeueQueueName = QueueName(s"${queueName.value}.requeue")
  def publish(body: Payload, properties: MessageProperties = MessageProperties.persistentBasic): F[Unit] = publisher(
    PublishCommand(exchangeName, RoutingKey(queueName.value), properties, body))
}

trait ChannelAmqpPurgeTest[F[_]] extends FunSuite with ScalaFutures with StrictLogging {
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  def withPublisher(testQueueName: QueueName = Any.randomQueue(), requeueStrategy: RequeueStrategy[F] = NoneHandler, shouldDeclare: Boolean = true)(f: TestFixture[F] => Unit): Unit

  def verifySuccess(f: F[Unit]): Assertion



  test(s"Can purge messages from a queue") {
    val queueName = Any.randomQueue()

    withPublisher(queueName) { app =>
      logger.info(s"Publish message on $queueName")
      verifySuccess(app.publisher(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, Any.randomPayload())))
    }
    withPublisher(queueName, shouldDeclare = false) { app =>
      Eventually.eventually {
        logger.info(s"Estimate message on $queueName")
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(1)
      }
      logger.info(s"Purge $queueName")
      app.amqpClient.performOps(amqpOps => amqpOps.purgeQueue(queueName))
      Eventually.eventually {
        logger.info(s"Estimate message on $queueName")
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(0)
      }
    }
  }

}
