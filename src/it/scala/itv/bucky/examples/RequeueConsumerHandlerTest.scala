package itv.bucky.examples

import itv.bucky.{Ack, Requeue}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Matchers._

import scala.util.Random

class RequeueConsumerHandlerTest extends FunSuite with ScalaFutures {

  test("should Ack Banana messages") {
    RequeueIfNotBananaHandler(Fruit("Banana")).futureValue shouldBe Ack
  }

  test("should requeue any other message") {
    val message = Random.nextString(10)
    RequeueIfNotBananaHandler(Fruit(message)).futureValue shouldBe Requeue
  }

}
