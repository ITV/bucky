package itv.bucky

import itv.bucky.UnmarshalResult._
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import org.scalatest.Matchers._

import SameThreadExecutionContext.implicitly

class RabbitSimulatorTest extends FunSuite with ScalaFutures {

  test("Can publish and consume via simulator") {
    val rabbit = new RabbitSimulator()
    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    rabbit.publish(Payload.from("Hello"))(RoutingKey("my.routing.key")).futureValue shouldBe Ack
    rabbit.publish(Payload.from("world"))(RoutingKey("my.routing.key")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 2

    messages.head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    messages.last.body.unmarshal[String] shouldBe "world".unmarshalSuccess
  }

  test("it should not able to ack when the routing key does not found a queue") {
    val rabbit = new RabbitSimulator()
    val result = rabbit.publish(Payload.from("Foo"))(RoutingKey("invalid.routing.key")).failed.futureValue

    result.getMessage should include("No consumers found")
  }

  test("Can publish and consume via simulator with headers") {
    val rabbit = new RabbitSimulator()
    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    rabbit.publish(Payload.from("Hello"))(RoutingKey("my.routing.key"), Map("my.header"->"hello")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 1

    val head = messages.head
    head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    head.properties.headers("my.header") shouldBe "hello"
  }

  test("Can publish and consume via simulator without headers") {
    val rabbit = new RabbitSimulator()
    val messages = rabbit.watchQueue(QueueName("my.routing.key"))

    rabbit.publish(Payload.from("Hello"))(RoutingKey("my.routing.key")).futureValue shouldBe Ack

    rabbit.waitForMessagesToBeProcessed()(1.second)
    messages should have size 1

    val head = messages.head
    head.body.unmarshal[String] shouldBe "Hello".unmarshalSuccess
    head.properties.headers.get("my.header") shouldBe None
  }

  test("Can publish and consume via simulator with a defined MapExchange or else use the Identity Exchange") {
    val rabbit = new RabbitSimulator(Map(RoutingKey("a") -> QueueName("b")) orElse IdentityBindings)

    val aTobMessages = rabbit.watchQueue(QueueName("b"))
    aTobMessages shouldBe 'empty

    rabbit.publish(Payload.from("a to b"))(RoutingKey("a")).futureValue shouldBe Ack
    aTobMessages should have size 1
    aTobMessages.head.body.unmarshal[String] shouldBe "a to b".unmarshalSuccess

    val cMessages = rabbit.watchQueue(QueueName("c"))
    cMessages shouldBe 'empty

    rabbit.publish(Payload.from("c to c"))(RoutingKey("c")).futureValue shouldBe Ack

    cMessages should have size 1
    cMessages.head.body.unmarshal[String] shouldBe "c to c".unmarshalSuccess
  }



}
