package itv.bucky.examples

import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky._
import itv.bucky.examples.BasicConsumer.{BasicConsumerLifecycle, Config}
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}
import itv.bucky.SameThreadExecutionContext.implicitly
import UnmarshalResult._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import itv.bucky.RabbitSimulator

import scala.collection.mutable.ListBuffer

class BasicConsumerTest extends FunSuite with ScalaFutures {

  test("it should requeue a message") {
    Lifecycle.using(testLifecycle) { app =>
      app.publisher(MyMessage("Hello")).futureValue


      app.requeueMessages should have size 1
      val message = app.requeueMessages.head
      message.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    }
  }

  test("it should ack a message") {
    import RabbitSimulator._
    Lifecycle.using(testLifecycle) { app =>
      app.publisher(MyMessage("Foo")).futureValue

      val commandBuilder = defaultPublishCommandBuilder using RoutingKey("bucky-basicconsumer-example")
      app.rabbit.publish(commandBuilder.toPublishCommand("Foo")).futureValue shouldBe Ack

      app.requeueMessages should have size 0
    }
  }

  case class AppFixture(rabbit: RabbitSimulator, publisher: Publisher[MyMessage], requeueMessages: ListBuffer[Delivery])

  def testLifecycle: Lifecycle[AppFixture] = {
    val amqpClient = new RabbitSimulator()
    val requeueMessages = amqpClient.watchQueue(QueueName("bucky-basicconsumer-example.requeue"))


    val messageMarshaller: PayloadMarshaller[MyMessage] = StringPayloadMarshaller.contramap(_.foo)

    import itv.bucky.PublishCommandBuilder._
    val myMessageSerializer = publishCommandBuilder(messageMarshaller) using RoutingKey("bucky-basicconsumer-example") using ExchangeName("")

    import AmqpClient._
    for {
      publisher <- amqpClient.publisher().map(publisherOf[MyMessage](myMessageSerializer))
      _ <- new BasicConsumerLifecycle {
        protected override def buildAmqpClient(amqpClientConfig: AmqpClientConfig): Lifecycle[AmqpClient] = {
          NoOpLifecycle(amqpClient)
        }
      }.apply(Config(QueueName("bucky-basicconsumer-example"),QueueName("bucky-basicconsumer-example.requeue"))) //FIXME Change the requeue name to target
    } yield AppFixture(amqpClient, publisher, requeueMessages)
  }

}