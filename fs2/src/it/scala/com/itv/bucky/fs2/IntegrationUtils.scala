package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue._
import com.itv.lifecycle.Lifecycle
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Assertion

import scala.concurrent.duration._
import org.scalatest.Matchers._
import _root_.fs2._

trait IOEffectVerification extends EffectVerification[IO] {

  def verifySuccess(f: IO[Unit]): Assertion = f.unsafeRunSync() should ===(())

}

trait IOPublisherConsumerBaseTest extends PublisherConsumerBaseTest[IO] with IOEffectVerification {

  override def withPublisherAndConsumer(queueName: QueueName, requeueStrategy: RequeueStrategy[IO])(
      f: (TestFixture[IO]) => Unit): Unit =
    IntegrationUtils.withPublisherAndConsumer(queueName, requeueStrategy)(f)
}

object IntegrationUtils extends StrictLogging {
  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

  def config: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    AmqpClientConfig(config.getString("rmq.host"),
                     config.getInt("rmq.port"),
                     config.getString("rmq.username"),
                     config.getString("rmq.password"))
  }

  protected def withPublihserAndAmqpClient(
      testQueueName: QueueName = Any.queue(),
      requeueStrategy: RequeueStrategy[IO] = NoneHandler,
      shouldDeclare: Boolean = true)(f: (IOAmqpClient, TestFixture[IO]) => Unit): Unit = {
    val routingKey = RoutingKey(testQueueName.value)
    val exchange   = ExchangeName("")
    import com.itv.bucky.future.SameThreadExecutionContext.implicitly // TODO Review it

    Lifecycle.using(IOAmqpClientLifecycle(IntegrationUtils.config)) { client =>
      val declaration = requeueStrategy match {
        case NoneRequeue(_) => defaultDeclaration(testQueueName)
        case SimpleRequeue(_) =>
          basicRequeueDeclarations(testQueueName, retryAfter = 1.second) collect {
            case ex: Exchange => ex.autoDelete.expires(1.minute)
            case q: Queue     => q.autoDelete.expires(1.minute)
          }
        case _ =>
          logger.debug(s"Requeue declarations")
          requeueDeclarations(testQueueName,
                              RoutingKey(testQueueName.value),
                              Exchange(ExchangeName(s"${testQueueName.value}.dlx")),
                              retryAfter = 1.second) collect {
            case ex: Exchange => ex.autoDelete.expires(1.minute)
            case q: Queue     => q.autoDelete.expires(1.minute)
          }
      }
      if (shouldDeclare)
        DeclarationExecutor(declaration, client, 5.seconds)

      val publisher: Publisher[IO, PublishCommand] = client.publisher()
      f(client, TestFixture(publisher, routingKey, exchange, testQueueName, client))

      logger.debug(s"Closing the the publisher")
    }
  }

  def withPublisher(testQueueName: QueueName = Any.queue(),
                    requeueStrategy: RequeueStrategy[IO] = NoneHandler,
                    shouldDeclare: Boolean = true)(f: TestFixture[IO] => Unit): Unit =
    withPublihserAndAmqpClient(testQueueName, requeueStrategy, shouldDeclare) { (_, t) =>
      f(t)
    }

  def withPublisherAndConsumer(queueName: QueueName = Any.queue(), requeueStrategy: RequeueStrategy[IO])(
      f: TestFixture[IO] => Unit): Unit =
    withPublihserAndAmqpClient(queueName, requeueStrategy) {
      case (amqpClient: IOAmqpClient, t) =>
        withPublisher(queueName, requeueStrategy = requeueStrategy) { app =>
          val dlqHandler = requeueStrategy match {
            case NoneHandler    => None
            case NoneRequeue(_) => None
            case _ =>
              logger.debug(s"Create dlq handler")
              val dlqHandler   = new StubConsumeHandler[IO, Delivery]
              val dlqQueueName = QueueName(s"${queueName.value}.dlq")
              val consumer     = amqpClient.consumer(dlqQueueName, dlqHandler)
              consumer.run.unsafeRunAsync { _ =>
                }

              Some(dlqHandler)
          }

          val consumer: Stream[IO, Unit] = requeueStrategy match {
            case NoneHandler => Stream.empty
            case RawRequeue(requeueHandler, requeuePolicy) =>
              amqpClient.requeueOf(app.queueName, requeueHandler, requeuePolicy)
            case TypeRequeue(requeueHandler, requeuePolicy, unmarshaller) =>
              amqpClient.requeueHandlerOf(app.queueName, requeueHandler, requeuePolicy, unmarshaller)
            case SimpleRequeue(handler) => amqpClient.consumer(app.queueName, handler)
            case NoneRequeue(handler)   => amqpClient.consumer(app.queueName, handler)
          }

          consumer.run.unsafeRunAsync { _ =>
            }

          f(app.copy(dlqHandler = dlqHandler))
        }
    }

}
