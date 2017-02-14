package itv.bucky.examples

import com.itv.bucky.StubPublisher
import itv.bucky.{Ack, Requeue}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Matchers._

import scala.util.Random

class RequeueIfNotBananaHandlerTest extends FunSuite with ScalaFutures {

  test("should Ack Banana messages") {
    val stubPublisher = new StubPublisher[DeliveryRequest]
    RequeueIfNotBananaHandler(stubPublisher)(Fruit("Banana")).futureValue shouldBe Ack
    stubPublisher.publishedEvents shouldBe 'nonEmpty
  }

  test("should requeue any other message") {
    val message = Random.nextString(10)
    val stubPublisher = new StubPublisher[DeliveryRequest]
    RequeueIfNotBananaHandler(stubPublisher)(Fruit(message)).futureValue shouldBe Requeue
    stubPublisher.publishedEvents shouldBe 'empty
  }

}
