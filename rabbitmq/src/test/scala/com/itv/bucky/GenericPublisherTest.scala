package com.itv.bucky

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

class GenericPublisherTest extends FunSuite with ScalaFutures {

  import com.itv.bucky.SameThreadExecutionContext.implicitly

  test("Publishing only returns success once publication is acknowledged") {
    val client = createClient()
    val expectedExchange = ExchangeName("")
    val expectedRoutingKey = RoutingKey("mymessage")
    val expectedProperties = MessageProperties.textPlain

    val expectedBody = Payload.from("expected message")

    object Banana

    import PublishCommandBuilder._

    implicit val bananaMarshaller: PayloadMarshaller[Banana.type] = PayloadMarshaller(_ => expectedBody)
    implicit val bananaSerializer =
      publishCommandBuilder(bananaMarshaller) using expectedExchange using expectedRoutingKey using expectedProperties

    val publish = AmqpClient.publisherOf(bananaSerializer)(client)
    val result = publish(Banana)

    result shouldBe 'completed
    result.futureValue.shouldBe(())

    client.publishedEvents should have size 1

    val message = client.publishedEvents.head
    message.exchange shouldBe expectedExchange
    message.routingKey shouldBe expectedRoutingKey
    message.basicProperties shouldBe expectedProperties
    message.body shouldBe expectedBody
  }

  test("publishing with a broken publish command serializer should fail the publish future") {
    object Banana
    val expectedException = new RuntimeException("What's a banana?")


    implicit val serializer = new PublishCommandBuilder[Banana.type] {
      def toPublishCommand(t: Banana.type): PublishCommand =
        throw expectedException
    }

    val client = createClient()

    val publish = AmqpClient.publisherOf[Banana.type](serializer)(client)
    val result = publish(Banana).failed.futureValue

    result shouldBe expectedException
  }


  private def createClient(): StubPublisher[PublishCommand] = {
    new StubPublisher[PublishCommand]()
  }
}
