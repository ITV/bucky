package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.PublishCommandBuilder._
import com.itv.bucky._
import com.itv.bucky.fs2.TestIOExt._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._

class GenericPublisherTest extends FunSuite with ScalaFutures {

  implicit val publisherPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  test("Publishing only returns success once publication is acknowledged") {

    val client = createClient()

    val expectedExchange = ExchangeName("")
    val expectedRoutingKey = RoutingKey("mymessage")
    val expectedProperties = MessageProperties.textPlain

    val expectedBody = Payload.from("expected message")

    object Banana

    implicit val bananaMarshaller: PayloadMarshaller[Banana.type] = PayloadMarshaller(_ => expectedBody)
    implicit val bananaSerializer =
      publishCommandBuilder(bananaMarshaller) using expectedExchange using expectedRoutingKey using expectedProperties

    val publisher = AmqpClient.publisherOf(bananaSerializer)(client)
    val result = publisher(Banana)

    eventually {
      val status = result.status
      status shouldBe 'completed
      status shouldBe 'success
    }

    client.publishedEvents should have size 1

    val message = client.publishedEvents.head
    message.exchange shouldBe expectedExchange
    message.routingKey shouldBe expectedRoutingKey
    message.basicProperties shouldBe expectedProperties
    message.body shouldBe expectedBody
  }

  test("Publishing with a broken publish command serializer should fail the publish") {
    object Banana
    val expectedException = new RuntimeException("What's a banana?")

    implicit val serializer = new PublishCommandBuilder[Banana.type] {
      def toPublishCommand(t: Banana.type): PublishCommand =
        throw expectedException
    }

    val client = createClient()

    val publisher = AmqpClient.publisherOf[IO, Banana.type](serializer)(client)
    val result = publisher(Banana)

    eventually {
      val status = result.status
      status shouldBe 'failure
      status.failure shouldBe a[expectedException.type]
      status.failure.getMessage shouldBe expectedException.getMessage
    }
  }

  private def createClient(): StubPublisher[IO, PublishCommand] = {
    new StubPublisher[IO, PublishCommand]()
  }
}
