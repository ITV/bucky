package com.itv.bucky.taskz

import com.itv.bucky._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import PublishCommandBuilder._

import scalaz.concurrent.Task
import org.scalatest.concurrent.Eventually._
import scala.concurrent.duration._

class GenericPublisherTest extends FunSuite with ScalaFutures {

  import TaskExt._
  implicit val publisherPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)

  test("Publishing only returns success once publication is acknowledged") {
    val client = createClient()

    val expectedExchange   = ExchangeName("")
    val expectedRoutingKey = RoutingKey("mymessage")
    val expectedProperties = MessageProperties.textPlain

    val expectedBody = Payload.from("expected message")

    object Banana

    implicit val bananaMarshaller: PayloadMarshaller[Banana.type] = PayloadMarshaller(_ => expectedBody)
    implicit val bananaSerializer =
      publishCommandBuilder(bananaMarshaller) using expectedExchange using expectedRoutingKey using expectedProperties

    val publish = AmqpClient.publisherOf(bananaSerializer)(client)
    val result  = resultFrom(publish(Banana))

    eventually {
      result shouldBe 'completed
      result shouldBe 'success
    }

    client.publishedEvents should have size 1

    val message = client.publishedEvents.head
    message.exchange shouldBe expectedExchange
    message.routingKey shouldBe expectedRoutingKey
    message.basicProperties shouldBe expectedProperties
    message.body shouldBe expectedBody
  }

  test("Publishing with a broken publish command serializer should fail the publish future") {
    object Banana
    val expectedException = new RuntimeException("What's a banana?")

    implicit val serializer = new PublishCommandBuilder[Banana.type] {
      def toPublishCommand(t: Banana.type): PublishCommand =
        throw expectedException
    }

    val client = createClient()

    val publish = AmqpClient.publisherOf[Task, Banana.type](serializer)(client)
    val result  = resultFrom(publish(Banana))

    eventually {
      result.failure shouldBe expectedException
    }
  }

  private def createClient(): StubPublisher[Task, PublishCommand] =
    new StubPublisher[Task, PublishCommand]()
}
