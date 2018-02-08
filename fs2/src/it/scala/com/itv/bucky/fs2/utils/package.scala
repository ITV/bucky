package com.itv.bucky.fs2

import _root_.fs2._
import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue._
import com.itv.bucky.suite._
import com.itv.lifecycle.Lifecycle
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Assertion
import org.scalatest.Matchers._

import scala.concurrent.duration._

package object utils {
  trait IOEffectVerification extends EffectVerification[IO] {

    def verifySuccess(f: IO[Unit]): Assertion = f.unsafeRunSync() should ===(())
    def verifyFailure(f: IO[Unit]): Assertion = f.attempt.unsafeRunSync() shouldBe 'left

  }

  trait IOEffectMonad extends EffectMonad[IO, Throwable] {
    override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError
  }

  trait IOPublisherBaseTest extends PublisherBaseTest[IO] with PublisherAndAmqpClient {

    def withPublisher(testQueueName: QueueName = Any.queue(),
                      requeueStrategy: RequeueStrategy[IO] = NoneHandler,
                      shouldDeclare: Boolean = true)(f: TestFixture[IO] => Unit): Unit =
      withPublisherAndAmqpClient(testQueueName, requeueStrategy, shouldDeclare) { (_, t) =>
        f(t)
      }
  }

  trait IOPublisherConsumerBaseTest
      extends PublisherConsumerBaseTest[IO]
      with PublisherAndAmqpClient
      with IOPublisherBaseTest
      with StrictLogging {

    def withPublisherAndConsumer(queueName: QueueName = Any.queue(), requeueStrategy: RequeueStrategy[IO])(
        f: TestFixture[IO] => Unit): Unit =
      withPublisherAndAmqpClient(queueName, requeueStrategy) {
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
                consumer.compile.drain.unsafeRunAsync { _ =>
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

            consumer.compile.drain.unsafeRunAsync { _ =>
              }

            f(app.copy(dlqHandler = dlqHandler))
          }
      }
  }

  def config: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    AmqpClientConfig(config.getString("rmq.host"),
                     config.getInt("rmq.port"),
                     config.getString("rmq.username"),
                     config.getString("rmq.password"))
  }

  trait PublisherAndAmqpClient extends StrictLogging {
    def defaultDeclaration(queueName: QueueName): List[Queue] =
      List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

    def withPublisherAndAmqpClient(testQueueName: QueueName = Any.queue(),
                                   requeueStrategy: RequeueStrategy[IO] = NoneHandler,
                                   shouldDeclare: Boolean = true)(f: (IOAmqpClient, TestFixture[IO]) => Unit): Unit = {
      import com.itv.bucky.future.SameThreadExecutionContext.implicitly
      val routingKey = RoutingKey(testQueueName.value)
      val exchange   = ExchangeName("")

      val declarations =
        if (shouldDeclare)
          declarationsFor(testQueueName, requeueStrategy)
        else List.empty

      IOAmqpClient
        .use(config, declarations) { client =>
          Stream.eval(
            IO {
              val publisher: Publisher[IO, PublishCommand] = client.publisher()
              f(client, TestFixture(publisher, routingKey, exchange, testQueueName, client))
            }
          )
        }
        .compile
        .last
        .unsafeRunSync()
    }

    private def declarationsFor(testQueueName: QueueName, requeueStrategy: RequeueStrategy[IO]) = {
      val declarations = requeueStrategy match {
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
      declarations.toList
    }
  }
}
