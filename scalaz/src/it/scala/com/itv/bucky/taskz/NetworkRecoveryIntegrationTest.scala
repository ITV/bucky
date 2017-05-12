package com.itv.bucky.taskz

import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky._
import com.itv.bucky.decl.{DeclarationExecutor, Queue}
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random
import scalaz.concurrent.Task
import org.scalatest.Matchers._

import scalaz.\/-


class NetworkRecoveryIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {

  import Eventually.eventually

  val success = \/-(())
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test("can recover publishers and consumers from a network failure") {
    withProxyConfigured { case (proxy, handlerA, handlerB, publisherA, publisherB) =>
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
          publisherB.apply(()).unsafePerformSyncAttempt shouldBe success
        }
      }

      withClue("should be able to consume from Queue B after broker allows connections again") {
        eventually {
          handlerB.receivedMessages should have size 3
        }
      }

      withClue("should be able to consume from Queue A after broker allows connections again") {
        eventually {
          handlerA.receivedMessages should have size 3
        }
      }
    }

  }


  def withProxyConfigured(f: (Proxy, StubConsumeHandler[Task, Unit], StubConsumeHandler[Task, Unit], Publisher[Task, Unit], Publisher[Task, Unit]) => Unit) = {

    val freePort = Port.randomPort()
    val amqpClientConfig = IntegrationUtils.config
    val proxyConfig = amqpClientConfig.copy(host = "localhost", port = freePort, networkRecoveryInterval = Some(1.second))
    val lifecycle = for {
      proxy <- ProxyLifecycle.apply(local = HostPort("localhost", freePort), remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      client <- DefaultTaskAmqpClientLifecycle(proxyConfig)
    } yield (proxy, client)


    Lifecycle.using(lifecycle) { case (proxy, client) =>
      val queueA = QueueName("proxy" + Random.nextInt())
      val queueB = QueueName("proxy" + Random.nextInt())
      val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))
      val pcbA = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueA.value)
      val pcbB = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueB.value)
      val handlerA = new StubConsumeHandler[Task, Unit]
      val handlerB = new StubConsumeHandler[Task, Unit]
      val unmarshaller = Unmarshaller.liftResult[Payload, Unit] {
        _.unmarshal[String] match {
          case UnmarshalResult.Success(s) if s == "hello" => UnmarshalResult.Success(())
          case UnmarshalResult.Success(other) => UnmarshalResult.Failure(other + " was not hello")
          case failure: UnmarshalResult.Failure => failure
        }
      }
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
      f(proxy, handlerA, handlerB, publisherA, publisherB)
    }
  }

}
