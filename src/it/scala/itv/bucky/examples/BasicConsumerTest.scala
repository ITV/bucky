package itv.bucky.examples

import itv.bucky._
import itv.bucky.examples.BasicConsumer.{BasicConsumerLifecycle, Config}
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.{Blob, BlobMarshaller}
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
      message.to[String] shouldBe "Hello"
    }
  }

  test("it should ack a message") {
    Lifecycle.using(testLifecycle) { app =>
      app.publisher(MyMessage("Foo")).futureValue

      app.rabbit.publish(Blob.from("Foo"))(RoutingKey("bucky-basicconsumer-example")).futureValue shouldBe Ack

      app.requeueMessages should have size 0
    }
  }

  case class AppFixture(rabbit: RabbitSimulator, publisher: Publisher[MyMessage], requeueMessages: ListBuffer[Blob])

  def testLifecycle: Lifecycle[AppFixture] = {
    val amqpClient = new RabbitSimulator()
    val requeueMessages = amqpClient.watchQueue("bucky-basicconsumer-example.requeue")


    implicit val messageMarshaller = BlobMarshaller[MyMessage] {
      message => Blob.from(message.foo)
    }
    import itv.bucky.BlobSerializer._
    implicit val foo = blobSerializer[MyMessage] using RoutingKey("bucky-basicconsumer-example") using Exchange("")

    import AmqpClient._
    for {
      publisher <- amqpClient.publisher().map(publisherOf[MyMessage])
      _ <- new BasicConsumerLifecycle {
        protected override def buildAmqpClient(amqpClientConfig: AmqpClientConfig): Lifecycle[AmqpClient] = {
          NoOpLifecycle(amqpClient)
        }
      }.apply(Config("bucky-basicconsumer-example"))
    } yield AppFixture(amqpClient, publisher, requeueMessages)
  }

}