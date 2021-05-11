package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import com.itv.bucky.wiring._
import com.itv.bucky.decl._
import com.itv.bucky.pattern.requeue._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class WiringTest extends AnyFunSuite with StrictLogging {

  object TestWiring extends Wiring[String](WiringName("test"))

  object CustomTestWiring
      extends Wiring[String](
        name = WiringName("test"),
        setExchangeName = Some(ExchangeName("exchange")),
        setRoutingKey = Some(RoutingKey("route")),
        setQueueName = Some(QueueName("queue")),
        setExchangeType = Some(Direct),
        setRequeuePolicy = Some(RequeuePolicy(10, 1.hour)),
        setPrefetchCount = Some(100)
      )

  test("A wiring should generate an exchange name") {
    TestWiring.exchangeName shouldBe ExchangeName("bucky.exchange.test")
  }
  test("it should take a custom exchange name") {
    CustomTestWiring.exchangeName shouldBe ExchangeName("exchange")
  }
  test("it should generate a routing key") {
    TestWiring.routingKey shouldBe RoutingKey("bucky.route.test")
  }
  test("it should take a custom routing key") {
    CustomTestWiring.routingKey shouldBe RoutingKey("route")
  }
  test("it should generate a queue name") {
    TestWiring.queueName shouldBe QueueName("bucky.queue.test")
  }
  test("it should take a custom queue name") {
    CustomTestWiring.queueName shouldBe QueueName("queue")
  }
  test("it should generate an exchange type") {
    TestWiring.exchangeType shouldBe Topic
  }
  test("it should take a custom exchange type") {
    CustomTestWiring.exchangeType shouldBe Direct
  }
  test("it should generate a requeue policy") {
    TestWiring.requeuePolicy shouldBe RequeuePolicy(maximumProcessAttempts = 10, 1.seconds)
  }
  test("it should take a custom requeue policy") {
    CustomTestWiring.requeuePolicy shouldBe RequeuePolicy(maximumProcessAttempts = 10, 1.hour)
  }
  test("it should generate a prefetch count") {
    TestWiring.prefetchCount shouldBe 1
  }
  test("it should take a custom prefetch count") {
    CustomTestWiring.prefetchCount shouldBe 100
  }
  test("it should create an exchange") {
    CustomTestWiring.exchange shouldBe Exchange(ExchangeName("exchange"), Direct)
  }
  test("it should create an exchange with binding") {
    CustomTestWiring.exchangeWithBinding shouldBe
      Exchange(ExchangeName("exchange"), Direct)
        .binding(RoutingKey("route") -> QueueName("queue"))
  }
  test("it should create declarations for publishers") {
    CustomTestWiring.publisherDeclarations shouldBe List(Exchange(ExchangeName("exchange")))
  }
  test("it should create declarations for consumers with requeue exchange type of Fanout by default") {
    val routingKey = RoutingKey("route")
    val queueName  = QueueName("queue")

    CustomTestWiring.consumerDeclarations shouldBe
      List(
        Exchange(ExchangeName("exchange"), Direct)
          .binding(routingKey -> queueName)
      ) ++
        requeueDeclarations(QueueName("queue"),
                            RoutingKey("-"),
                            Some(ExchangeName(s"${queueName.value}.dlx")),
                            Fanout,
                            CustomTestWiring.requeuePolicy.requeueAfter)
  }

  test("it should set the requeue ttl to 5 minutes where the requeue policy has a wait time of lower than 5 minutes") {
    val routingKey = RoutingKey("route")
    val queueName  = QueueName("queue")

    object CustomTestWiring
        extends Wiring[String](
          name = WiringName("test"),
          setExchangeName = Some(ExchangeName("exchange")),
          setRoutingKey = Some(RoutingKey("route")),
          setQueueName = Some(QueueName("queue")),
          setExchangeType = Some(Direct),
          setPrefetchCount = Some(100),
          setRequeuePolicy = Some(RequeuePolicy(10, 1.minute)),
        )

    CustomTestWiring.consumerDeclarations shouldBe
      List(
        Exchange(ExchangeName("exchange"), Direct)
          .binding(routingKey -> queueName)
      ) ++
        requeueDeclarations(QueueName("queue"), RoutingKey("-"), Some(ExchangeName(s"${queueName.value}.dlx")), Fanout, 5.minutes)
  }

  test("it should allow specification of dead letter exchange type") {
    val routingKey = RoutingKey("route")
    val dlxType    = Direct
    val queueName  = QueueName("queue")
    object CustomTestWiring
        extends Wiring[String](
          name = WiringName("test"),
          setExchangeName = Some(ExchangeName("exchange")),
          setRoutingKey = Some(routingKey),
          setQueueName = Some(queueName),
          setExchangeType = Some(Direct),
          setRequeuePolicy = Some(RequeuePolicy(10, 1.hour)),
          setPrefetchCount = Some(100),
          setDeadLetterExchangeType = Some(dlxType)
        )
    CustomTestWiring.consumerDeclarations shouldBe
      List(
        Exchange(ExchangeName("exchange"), Direct)
          .binding(routingKey -> QueueName("queue"))
      ) ++
        requeueDeclarations(QueueName("queue"),
                            routingKey,
                            Some(ExchangeName(s"${queueName.value}.dlx")),
                            dlxType,
                            CustomTestWiring.requeuePolicy.requeueAfter)
  }

}
