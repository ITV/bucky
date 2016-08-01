package itv.bucky

import itv.bucky.PublishCommandBuilder._
import itv.bucky.SameThreadExecutionContext.implicitly
import itv.bucky.decl.{DeclarationLifecycle, Queue}
import itv.contentdelivery.lifecycle.Lifecycle
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random


class NetworkRecoveryIntegrationTest extends FunSuite with ScalaFutures {
  import TestUtils._

  def testLifecycle: Lifecycle[(Proxy, StubConsumeHandler[Unit], StubConsumeHandler[Unit], Publisher[Unit], Publisher[Unit])] = {
    val queueA = QueueName("proxy" + Random.nextInt())
    val queueB = QueueName("proxy" + Random.nextInt())
    val amqpClientConfig = IntegrationUtils.config

    val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))
    val pcbA = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueA.value)
    val pcbB = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueB.value)
    val handlerA = new StubConsumeHandler[Unit]
    val handlerB = new StubConsumeHandler[Unit]
    val unmarshaller = Unmarshaller.liftResult[Payload, Unit] {
      _.unmarshal[String] match {
        case UnmarshalResult.Success(s) if s == "hello" => UnmarshalResult.Success(())
        case UnmarshalResult.Success(other) => UnmarshalResult.Failure(other + " was not hello")
        case failure: UnmarshalResult.Failure => failure
      }
    }

    val proxyConfig = amqpClientConfig.copy(host = "localhost", port = 9999, networkRecoveryInterval = Some(1.second))
    for {
      proxy <- ProxyLifecycle(local = HostPort("localhost", 9999), remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      client <- AmqpClientLifecycle(proxyConfig)
      _ <- DeclarationLifecycle(List(Queue(queueA).notDurable.expires(1.minute)), client)
      _ <- DeclarationLifecycle(List(Queue(queueB).notDurable.expires(1.minute)), client)
      publisherA <- client.publisherOf(pcbA)
      publisherB <- client.publisherOf(pcbB)
      _ <- client.consumer(queueA, AmqpClient.handlerOf(handlerA, unmarshaller))
      _ <- client.consumer(queueB, AmqpClient.handlerOf(handlerB, unmarshaller))
    }
      yield (proxy, handlerA, handlerB, publisherA, publisherB)
  }

  test("can recover publishers and consumers from a network failure") {
    Lifecycle.using(testLifecycle) { case (proxy, handlerA, handlerB, publisherA, publisherB) =>
      handlerA.receivedMessages shouldBe 'empty
      handlerB.receivedMessages shouldBe 'empty

      withClue("should be able to publish and consume a couple of messages") {
        publisherA.apply(()).asTry.futureValue shouldBe 'success
        publisherB.apply(()).asTry.futureValue shouldBe 'success

        eventually {
          handlerA.receivedMessages should have size 1
          handlerB.receivedMessages should have size 1
        }(Eventually.PatienceConfig(5.seconds, 1.second))

        publisherA.apply(()).asTry.futureValue shouldBe 'success
        publisherB.apply(()).asTry.futureValue shouldBe 'success

        eventually {
          handlerA.receivedMessages should have size 2
          handlerB.receivedMessages should have size 2
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      proxy.stopAcceptingNewConnections()
      proxy.closeAllOpenConnections()

      withClue("should fail to publish when connection to broker is lost") {
        publisherA.apply(()).asTry.futureValue shouldBe 'failure
        publisherB.apply(()).asTry.futureValue shouldBe 'failure
      }

      proxy.startAcceptingNewConnections()

      withClue("should be able to publish after broker allows connections again") {
        eventually {
          publisherA.apply(()).asTry.futureValue shouldBe 'success
          publisherB.apply(()).asTry.futureValue shouldBe 'success
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      withClue("should be able to consume from Queue B after broker allows connections again") {
        eventually {
          handlerB.receivedMessages should have size 3
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      withClue("should be able to consume from Queue A after broker allows connections again") {
        eventually {
          handlerA.receivedMessages should have size 3
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }
    }
  }

}
