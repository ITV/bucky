package com.itv.bucky

import com.itv.bucky.future.SameThreadExecutionContext.implicitly
import com.itv.bucky.UnmarshalResult.Success
import com.itv.bucky.Unmarshaller._
import com.itv.bucky.decl._
import com.itv.bucky.lifecycle._
import com.itv.bucky.future._
import com.itv.bucky.pattern.requeue._
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Inside._
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class ConsumerIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {
  import IntegrationUtils._
  import TestLifecycle._

  implicit val consumerPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)

  case class Message(value: String)

  val messageUnmarshaller = StringPayloadUnmarshaller map Message

  test(s"Can consume messages from a (pre-existing) queue") {

    val handler = new StubConsumeHandler[Future, Message]()

    Lifecycle.using(messageConsumer(QueueName("bucky-consumer-test"), handler, toDeliveryUnmarshaller(messageUnmarshaller))) { publisher =>
      handler.receivedMessages shouldBe 'empty

      val expectedMessage = "Hello World!"
      publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-test"), MessageProperties.textPlain, Payload.from(expectedMessage)))

      eventually {
        logger.info(s"Waiting Can consume messages from a (pre-existing) queue")
        handler.receivedMessages should have size 1

        handler.receivedMessages.head shouldBe Message(expectedMessage)
      }
    }
  }


  test("Can extract headers from consumed message") {
    import com.itv.bucky.UnmarshalResult._

    case class Bar(value: String)
    case class Baz(value: String)
    case class Foo(bar: Bar, baz: Baz)

    val barUnmarshaller: Unmarshaller[Delivery, Bar] =
      Unmarshaller liftResult { delivery =>
        if (delivery.properties.headers.contains("bar"))
          Bar(delivery.properties.headers("bar").toString).unmarshalSuccess
        else
          "delivery did not contain bar header".unmarshalFailure
      }

    val bazUnmarshaller: Unmarshaller[Delivery, Baz] =
      toDeliveryUnmarshaller(Unmarshaller liftResult (_.unmarshal[String].map(Baz)))

    val fooUnmarshaller: Unmarshaller[Delivery, Foo] =
      (barUnmarshaller zip bazUnmarshaller) map { case (bar, baz) => Foo(bar, baz) }

    val handler = new StubConsumeHandler[Future, Foo]

    Lifecycle.using(messageConsumer(QueueName("bucky-consumer-header-test"), handler, fooUnmarshaller)) { publisher =>
      handler.receivedMessages shouldBe 'empty

      val expected = Foo(Bar("bar"), Baz("baz"))

      val publishCommand = PublishCommand(ExchangeName(""),
        RoutingKey("bucky-consumer-header-test"),
        MessageProperties.persistentBasic.withHeader("bar" -> expected.bar.value),
        Payload.from(expected.baz.value))

      publisher(publishCommand).futureValue

      eventually {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head shouldBe expected
      }
    }
  }

  test("DeadLetter upon exception from handler") {
    val queueName = QueueName("exception-from-handler" + Random.nextInt())
    val handler = new StubConsumeHandler[Future, Delivery]()
    val dlqHandler = new QueueWatcher[Delivery]
    val config: AmqpClientConfig = IntegrationUtils.config.copy(networkRecoveryInterval = None)
    for {
      amqpClient <- AmqpClientLifecycle(config)
      declarations = basicRequeueDeclarations(queueName, retryAfter = 1.second) collect {
        case ex: Exchange => ex.autoDelete.expires(1.minute)
        case q: Queue => q.autoDelete.expires(1.minute)
      }
      _ <- DeclarationLifecycle(declarations, amqpClient)
      publisher <- amqpClient.publisher()

      _ <- amqpClient.consumer(queueName, handler)

      _ <- amqpClient.consumer(QueueName(s"${queueName.value}.dlq"), dlqHandler)
    } {

      dlqHandler.receivedMessages shouldBe 'empty
      handler.nextException = Some(new RuntimeException("Hello, world"))

      val expectedMessage = "Message to dlq"
      publisher(PublishCommand(ExchangeName(""), RoutingKey(queueName.value), MessageProperties.textPlain, Payload.from(expectedMessage)))

      eventually {
        handler.receivedMessages should have size 1
        dlqHandler.receivedMessages should have size 1
      }
    }
  }


  test("Can consume messages from a (pre-existing) queue with the raw consumer") {
    val handler = new StubConsumeHandler[Future, Delivery]()
    Lifecycle.using(rawConsumer(QueueName("bucky-consumer-raw-test"), handler)) {
      publisher =>
        handler.receivedMessages shouldBe 'empty

        val expectedMessage = "Hello World!"
        publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-raw-test"), MessageProperties.textPlain, Payload.from(expectedMessage)))

        eventually {
          handler.receivedMessages should have size 1
          inside(handler.receivedMessages.head) {
            case Delivery(body, _, _, _) => Payload(body.value).unmarshal[String] shouldBe Success(expectedMessage)
          }
        }
    }
  }

  test("Message headers are exposed to (raw) consumers") {
    val handler = new StubConsumeHandler[Future, Delivery]()
    Lifecycle.using(rawConsumer(QueueName("bucky-consumer-headers-test"), handler)) {
      publisher =>
        handler.receivedMessages shouldBe 'empty

        val expectedMessage = "Hello World!"

        val messageProperties = MessageProperties.textPlain.withHeader("hello" -> "world")


        publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-headers-test"), messageProperties, Payload.from(expectedMessage)))

        eventually {
          handler.receivedMessages should have size 1
          inside(handler.receivedMessages.head) {
            case Delivery(body, _, _, properties) => properties.headers.get("hello").map(_.toString) shouldBe Some("world")
          }
        }
    }
  }

  def messageConsumer[T](queueName: QueueName, handler: Handler[Future, T], unmarshaller: DeliveryUnmarshaller[T]) = for {
    result <- base(defaultDeclaration(queueName))
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, AmqpClient.deliveryHandlerOf(handler, unmarshaller))
  } yield publisher


}
