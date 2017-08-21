package com.itv.bucky.taskz

import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky._
import com.itv.bucky.decl.{DeclarationExecutor, Exchange, Queue, Topic}
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Random, Try}
import scalaz.\/-
import scalaz.concurrent.Task

class NetworkRecoveryIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {

  import Eventually.eventually
  import TaskExt._

  val success                           = \/-(())
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test("cannot recover connection from a network failure on start when the policy is not defined") {
    val (freePort, amqpClientConfig, proxyConfig) = config
    val lifecycle = for {
      proxy <- ProxyLifecycle.apply(local = HostPort("localhost", freePort),
                                    remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      _ = proxy.stopAcceptingNewConnections()
      _ = Task.schedule(proxy.startAcceptingNewConnections(), 100.millis).unsafePerformAsync(_ => ())
      client <- DefaultTaskAmqpClientLifecycle(proxyConfig.copy(networkRecoveryIntervalOnStart = None))
    } yield (proxy, client)

    Try(Lifecycle.using(lifecycle) {
      case (_, _) =>
    }) shouldBe 'failure
  }

  test("can recover connection from a network failure on start") {

    val (freePort, amqpClientConfig, proxyConfig) = config
    val queueA                                    = QueueName("task-proxy-start" + Random.nextInt())
    val exchangeName                              = ExchangeName("tmp")
    val routingKey                                = RoutingKey(queueA.value)

    val lifecycle = for {
      proxy <- ProxyLifecycle.apply(local = HostPort("localhost", freePort),
                                    remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      _ = proxy.stopAcceptingNewConnections()
      _ = Task.schedule(proxy.startAcceptingNewConnections(), 201.millis).unsafePerformAsync(_ => ())
      client <- DefaultTaskAmqpClientLifecycle(
        proxyConfig.copy(networkRecoveryIntervalOnStart = Some(NetworkRecoveryOnStart(100 millis, 1 second))))

      handler = createConsumer(
        client,
        exchangeName,
        routingKey,
        queueA
      )

    } yield (proxy, client, handler)

    Lifecycle.using(lifecycle) {
      case (proxy, client, handler) =>
        val pcbA    = publishCommandBuilder[Unit](marshaller) using exchangeName using routingKey
        val publish = client.publisherOf(pcbA).apply(())

        handler.receivedMessages shouldBe 'empty
        publish.unsafePerformSyncAttempt

        eventually {
          handler.receivedMessages should have size 1
        }
    }
  }

  test("can recover publishers and consumers from a network failure") {
    withProxyConfigured {
      case (proxy, handlerA, handlerB, publisherA, publisherB) =>
        handlerA.receivedMessages shouldBe 'empty
        handlerB.receivedMessages shouldBe 'empty

        withClue("should be able to publish and consume a couple of messages") {
          publisherA.apply(()).unsafePerformSyncAttempt shouldBe success
          publisherB.apply(()).unsafePerformSyncAttempt shouldBe success

          eventually {
            handlerA.receivedMessages should have size 1
            handlerB.receivedMessages should have size 1
          }

          publisherA.apply(()).unsafePerformSyncAttempt shouldBe success
          publisherB.apply(()).unsafePerformSyncAttempt shouldBe success

          eventually {
            handlerA.receivedMessages should have size 2
            handlerB.receivedMessages should have size 2
          }
        }

        proxy.stopAcceptingNewConnections()
        proxy.closeAllOpenConnections()

        withClue("should fail to publish when connection to broker is lost") {
          publisherA.apply(()).unsafePerformSyncAttempt should not be success
          publisherB.apply(()).unsafePerformSyncAttempt should not be success
        }

        proxy.startAcceptingNewConnections()

        withClue("should be able to publish after broker allows connections again") {
          eventually {
            publisherA.apply(()).unsafePerformSyncAttempt shouldBe success
          }
        }
        publisherB.apply(()).unsafePerformSyncAttempt shouldBe success

        withClue("should be able to consume from Queue A after broker allows connections again") {
          eventually {
            handlerA.receivedMessages should have size 3
          }
        }

        withClue("should be able to consume from Queue B after broker allows connections again") {
          eventually {
            handlerB.receivedMessages should have size 3
          }
        }

    }
  }

  def withProxyConfigured(
      f: (Proxy,
          StubConsumeHandler[Task, Unit],
          StubConsumeHandler[Task, Unit],
          Publisher[Task, Unit],
          Publisher[Task, Unit]) => Unit) = {
    import TaskExt._

    val (freePort, amqpClientConfig, proxyConfig) = config

    val lifecycle = for {
      proxy <- ProxyLifecycle.apply(local = HostPort("localhost", freePort),
                                    remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      client <- DefaultTaskAmqpClientLifecycle(proxyConfig)
    } yield (proxy, client)

    Lifecycle.using(lifecycle) {
      case (proxy, client) =>
        val queueA = QueueName("task-proxyA" + Random.nextInt())
        val queueB = QueueName("task-proxyB" + Random.nextInt())

        val pcbA = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueA.value)
        val pcbB = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueB.value)

        val handlerA = new StubConsumeHandler[Task, Unit]
        val handlerB = new StubConsumeHandler[Task, Unit]

        DeclarationExecutor(List(Queue(queueA).notDurable.expires(1.minute)), client)
        DeclarationExecutor(List(Queue(queueB).notDurable.expires(1.minute)), client)
        val publisherA = client.publisherOf(pcbA)
        val publisherB = client.publisherOf(pcbB)
        client.consumer(queueA, AmqpClient.handlerOf(handlerA, unmarshaller)).run.unsafePerformAsync { result =>
          logger.info(s"Closing consumer a: $result")
        }
        client.consumer(queueB, AmqpClient.handlerOf(handlerB, unmarshaller)).run.unsafePerformAsync { result =>
          logger.info(s"Closing consumer b: $result")
        }
        client.performOps(_.purgeQueue(queueA))
        client.performOps(_.purgeQueue(queueB))
        f(proxy, handlerA, handlerB, publisherA, publisherB)
    }
  }

  private def config = {
    val freePort         = Port.randomPort()
    val amqpClientConfig = IntegrationUtils.config
    val proxyConfig =
      amqpClientConfig.copy(host = "localhost", port = freePort, networkRecoveryInterval = Some(1.second))
    (freePort, amqpClientConfig, proxyConfig)
  }

  val unmarshaller = Unmarshaller.liftResult[Payload, Unit] {
    _.unmarshal[String] match {
      case UnmarshalResult.Success(s) if s == "hello" => UnmarshalResult.Success(())
      case UnmarshalResult.Success(other)             => UnmarshalResult.Failure(other + " was not hello")
      case failure: UnmarshalResult.Failure           => failure
    }
  }

  val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))

  def createConsumer(amqpClient: AbstractTaskAmqpClient.TaskAmqpClient,
                     exchangeName: ExchangeName,
                     routingKey: RoutingKey,
                     queueName: QueueName) = {

    val testDeclaration = List(
      Queue(queueName),
      Exchange(exchangeName, exchangeType = Topic)
        .binding(routingKey -> queueName)
    )

    DeclarationExecutor(testDeclaration, amqpClient)

    val stubConsumeHandler = new StubConsumeHandler[Task, Delivery]()

    amqpClient.consumer(queueName, stubConsumeHandler).run.unsafePerformAsync { _ =>
      }
    stubConsumeHandler
  }
}
