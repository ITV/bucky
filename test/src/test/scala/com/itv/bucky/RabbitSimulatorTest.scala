package com.itv.bucky

import PayloadMarshaller.StringPayloadMarshaller
import UnmarshalResult._
import com.itv.bucky.decl._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import com.itv.bucky.Monad._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class RabbitSimulatorTest extends FunSuite with ScalaFutures {

  import RabbitSimulator._

  implicit val executionContextExecutor = new ExecutionContextExecutor {
    override def execute(runnable: Runnable): Unit = runnable.run()

    override def reportFailure(cause: Throwable): Unit = throw cause

  }
  implicit val futureMonad = new MonadError[Future, Throwable] {
    override def apply[A](a: => A): Future[A] = Future(a)

    override def map[A, B](m: Future[A])(f: (A) => B): Future[B] = m.map(f)

    override def flatMap[A, B](m: Future[A])(f: (A) => Future[B]): Future[B] = m.flatMap(f)

    override def raiseError[A](e: Throwable): Future[A] = Future.failed(e)

    override def handleError[A](fa: Future[A])(f: (Throwable) => Future[A]): Future[A] = fa.recoverWith {
      case t: Throwable => f(t)
    }
  }

  test("Can publish and consume via simulator") {
    val rabbit = new RabbitSimulator[Id]()

    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    val commandBuilder = defaultPublishCommandBuilder using RoutingKey("my.routing.key")

    rabbit.publish(commandBuilder.toPublishCommand("Hello")).futureValue shouldBe Ack
    rabbit.publish(commandBuilder.toPublishCommand("world")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 2

    messages.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    messages.last.body.unmarshal[String] shouldBe "world".unmarshalSuccess
  }

  test("Can publish and consume in multiple queues via simulator") {
    val rabbit = new RabbitSimulator[Id]()

    val messages  = rabbit.watchQueue(QueueName("my.routing.key"))
    val messages2 = rabbit.watchQueue(QueueName("my.routing.key"))

    val commandBuilder = defaultPublishCommandBuilder using RoutingKey("my.routing.key")

    rabbit.publish(commandBuilder.toPublishCommand("Hello")).futureValue shouldBe Ack
    rabbit.publish(commandBuilder.toPublishCommand("world")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 2
    messages2 should have size 2

    messages.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    messages.last.body.unmarshal[String] shouldBe "world".unmarshalSuccess
    messages2.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    messages2.last.body.unmarshal[String] shouldBe "world".unmarshalSuccess
  }

  test("Can publish and consume via simulator with an exchange and different queues") {
    val rabbit             = new RabbitSimulator[Id]()
    val exchangeName       = ExchangeName("exchange")
    val firstQueueName     = QueueName("a")
    val firstRoutingQueue  = RoutingKey("exchage.a")
    val secondQueueName    = QueueName("b")
    val secondRoutingQueue = RoutingKey("exchage.b")

    rabbit.performOps { amqpOps =>
      for {
        _ <- amqpOps.bindQueue(Binding(exchangeName, firstQueueName, firstRoutingQueue, Map.empty))
        _ <- amqpOps.bindQueue(Binding(exchangeName, secondQueueName, secondRoutingQueue, Map.empty))
      } yield ()
    }

    val firstMessages  = rabbit.watchQueue(firstQueueName)
    val secondMessages = rabbit.watchQueue(secondQueueName)

    val commandBuilder = stringPublishCommandBuilder using exchangeName

    rabbit.publish((commandBuilder using firstRoutingQueue).toPublishCommand("Hello")).futureValue shouldBe Ack
    rabbit.publish((commandBuilder using secondRoutingQueue).toPublishCommand("world")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    firstMessages should have size 1
    secondMessages should have size 1

    firstMessages.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    secondMessages.head.body.unmarshal[String] shouldBe "world".unmarshalSuccess
  }

  test("it should not able to ack when the routing key does not found a queue") {
    val rabbit         = new RabbitSimulator[Id]()
    val commandBuilder = defaultPublishCommandBuilder using RoutingKey("invalid.routing.key")
    val result         = rabbit.publish(commandBuilder.toPublishCommand("Foo")).failed.futureValue

    result.getMessage should include("No consumers found")
  }

  test("Can publish and consume via simulator with different exchanges") {
    val rabbit             = new RabbitSimulator[Id]()
    val firstExchangeName  = ExchangeName("exchange1")
    val firstQueueName     = QueueName("a")
    val firstRoutingQueue  = RoutingKey("exchage1.a")
    val secondExchangeName = ExchangeName("exchange2")
    val secondQueueName    = QueueName("b")
    val secondRoutingQueue = RoutingKey("exchage.b")

    rabbit.performOps { amqpOps =>
      for {
        _ <- amqpOps.bindQueue(Binding(firstExchangeName, firstQueueName, firstRoutingQueue, Map.empty))
        _ <- amqpOps.bindQueue(Binding(secondExchangeName, secondQueueName, secondRoutingQueue, Map.empty))
      } yield ()
    }

    val firstMessages  = rabbit.watchQueue(firstQueueName)
    val secondMessages = rabbit.watchQueue(secondQueueName)

    val firstCommandBuilder  = stringPublishCommandBuilder using firstExchangeName
    val secondCommandBuilder = stringPublishCommandBuilder using secondExchangeName

    rabbit.publish((firstCommandBuilder using firstRoutingQueue).toPublishCommand("Hello")).futureValue shouldBe Ack
    rabbit.publish((secondCommandBuilder using secondRoutingQueue).toPublishCommand("world")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    firstMessages should have size 1
    secondMessages should have size 1

    firstMessages.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    secondMessages.head.body.unmarshal[String] shouldBe "world".unmarshalSuccess

  }

  test("It should pass publish headers through to the consumer") {
    val rabbit = new RabbitSimulator[Id]()

    val messages = rabbit.watchQueue(QueueName("queue.name"))

    val publisher = rabbit.publisher()
    val result = publisher(
      PublishCommand(ExchangeName(""),
                     RoutingKey("queue.name"),
                     MessageProperties.persistentBasic.withHeader("foo" -> "bar"),
                     Payload.from("")))

    result.futureValue shouldBe (())

    messages.size shouldBe 1

    messages.head.properties.headers("foo") shouldBe ("bar")
  }

  test("Can publish and consume via simulator with headers") {
    val rabbit   = new RabbitSimulator[Id]()
    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    val commandBuilder = defaultPublishCommandBuilder using RoutingKey("my.routing.key") using MessageProperties.minimalPersistentBasic
      .withHeader("my.header" -> "hello")

    rabbit.publish(commandBuilder.toPublishCommand("Hello")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 1

    val head = messages.head
    head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    head.properties.headers("my.header") shouldBe "hello"
  }

  test("Can publish and consume via simulator without headers") {
    val rabbit   = new RabbitSimulator[Id]()
    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    val commandBuilder = defaultPublishCommandBuilder using RoutingKey("my.routing.key")

    rabbit.publish(commandBuilder.toPublishCommand("Hello")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 1

    val head = messages.head
    head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    head.properties.headers.get("my.header") shouldBe None
  }

  test("Can publish and consume via simulator with a defined MapExchange or else use the Identity Exchange") {
    val rabbit = new RabbitSimulator[Id](Map(RoutingKey("a") -> QueueName("b")) orElse IdentityBindings)

    val aTobMessages = rabbit.watchQueue(QueueName("b"))
    aTobMessages shouldBe 'empty

    val aCommandBuilder = defaultPublishCommandBuilder using RoutingKey("a")

    rabbit.publish(aCommandBuilder.toPublishCommand("a to b")).futureValue shouldBe Ack
    aTobMessages should have size 1
    aTobMessages.head.body.unmarshal[String] shouldBe "a to b".unmarshalSuccess

    val cMessages = rabbit.watchQueue(QueueName("c"))
    cMessages shouldBe 'empty
    val cCommandBuilder = defaultPublishCommandBuilder using RoutingKey("c")

    rabbit.publish(cCommandBuilder.toPublishCommand("c to c")).futureValue shouldBe Ack

    cMessages should have size 1
    cMessages.head.body.unmarshal[String] shouldBe "c to c".unmarshalSuccess
  }

}
