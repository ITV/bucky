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

  def testLifecycle: Lifecycle[(Proxy, StubConsumeHandler[Unit], Publisher[Unit])] = {
    val queueName = QueueName("proxy" + Random.nextInt())
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))
    val pcb = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueName.value)
    val handler = new StubConsumeHandler[Unit]
    val unmarshaller = Unmarshaller.liftResult[Payload, Unit] {
      _.unmarshal[String] match {
        case UnmarshalResult.Success(s) if s == "hello" => UnmarshalResult.Success(())
        case UnmarshalResult.Success(other) => UnmarshalResult.Failure(other + " was not hello")
        case failure: UnmarshalResult.Failure => failure
      }
    }

    for {
      proxy <- ProxyLifecycle(local = HostPort("localhost", 9999), remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      client <- amqpClientConfig.copy(host = "localhost", port = 9999, networkRecoveryInterval = Some(1.second))
      _ <- DeclarationLifecycle(List(Queue(queueName).notDurable.expires(1.minute)), client)
      publisher <- client.publisherOf(pcb)
      _ <- client.consumer(queueName, AmqpClient.handlerOf(handler, unmarshaller))
    }
      yield (proxy, handler, publisher)
  }

  test("can recover publishers and consumers from a network failure") {
    Lifecycle.using(testLifecycle) { case (proxy, handler, publisher) =>
      handler.receivedMessages shouldBe 'empty

      withClue("should be able to publish and consume a couple of messages") {
        publisher.apply(()).asTry.futureValue shouldBe 'success

        eventually {
          handler.receivedMessages should have size 1
        }(Eventually.PatienceConfig(5.seconds, 1.second))

        publisher.apply(()).asTry.futureValue shouldBe 'success

        eventually {
          handler.receivedMessages should have size 2
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      proxy.stopAcceptingNewConnections()
      proxy.closeAllOpenConnections()

      withClue("should fail to publish when connection to broker is lost") {
        publisher.apply(()).asTry.futureValue shouldBe 'failure
      }

      proxy.startAcceptingNewConnections()

      withClue("should be able to publish after broker allows connections again") {
        eventually {
          publisher.apply(()).asTry.futureValue shouldBe 'success
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      withClue("should be able to consume after broker allows connections again") {
        eventually {
          handler.receivedMessages should have size 3
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }
    }
  }

}
