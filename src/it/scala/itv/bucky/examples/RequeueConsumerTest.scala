package itv.bucky.examples

import itv.bucky.{Ack, Consume, Requeue}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Matchers._

import scala.util.Random

class RequeueConsumerTest extends FunSuite with ScalaFutures {

  test("should Ack Martin messages") {
    RequeueIfNotMartinHandler(Person("Martin")).futureValue shouldBe Consume(Ack)
  }

  test("should requeue any other message") {
    val message = Random.nextString(10)
    RequeueIfNotMartinHandler(Person(message)).futureValue shouldBe Requeue
  }

}
