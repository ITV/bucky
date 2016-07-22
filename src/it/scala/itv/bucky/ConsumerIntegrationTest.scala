package itv.bucky

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.MessageProperties
import itv.bucky.UnmarshalResult.Success
import itv.bucky.Unmarshaller._
import itv.bucky.decl.{DeclarationLifecycle, Exchange, Queue}
import itv.bucky.pattern.requeue._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import org.scalatest.FunSuite
import org.scalatest.Inside._
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

class ConsumerIntegrationTest extends FunSuite with ScalaFutures {

  import IntegrationUtils._
  import TestLifecycle._

  case class Message(value: String)

  val messageUnmarshaller = StringPayloadUnmarshaller map Message

  test("Can consume messages from a (pre-existing) queue") {
    val handler = new StubConsumeHandler[Message]()

    Lifecycle.using(messageConsumer(QueueName("bucky-consumer-test"), handler, toDeliveryUnmarshaller(messageUnmarshaller))) { publisher =>
      handler.receivedMessages shouldBe 'empty

      val expectedMessage = "Hello World!"
      publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-test"), MessageProperties.MINIMAL_PERSISTENT_BASIC, Payload.from(expectedMessage)))

      eventually {
        handler.receivedMessages should have size 1

        handler.receivedMessages.head shouldBe Message(expectedMessage)
      }
    }
  }

  test("Can extract headers from consumed message") {
    import UnmarshalResult._

    case class Bar(value: String)
    case class Baz(value: String)
    case class Foo(bar: Bar, baz: Baz)

    val barUnmarshaller: Unmarshaller[Delivery, Bar] =
      Unmarshaller liftResult { delivery =>
        if (delivery.properties.getHeaders.containsKey("bar"))
          Bar(delivery.properties.getHeaders.get("bar").toString).unmarshalSuccess
        else
          "delivery did not contain bar header".unmarshalFailure
      }

    val bazUnmarshaller: Unmarshaller[Delivery, Baz] =
      toDeliveryUnmarshaller(Unmarshaller liftResult (_.unmarshal[String].map(Baz)))

    val fooUnmarshaller: Unmarshaller[Delivery, Foo] =
      (barUnmarshaller zip bazUnmarshaller) map { case (bar, baz) => Foo(bar, baz) }

    val handler = new StubConsumeHandler[Foo]

    Lifecycle.using(messageConsumer(QueueName("bucky-consumer-header-test"), handler, fooUnmarshaller)) { publisher =>
      handler.receivedMessages shouldBe 'empty

      val expected = Foo(Bar("bar"), Baz("baz"))

      val publishCommand = PublishCommand(ExchangeName(""),
        RoutingKey("bucky-consumer-header-test"),
        MessageProperties.MINIMAL_PERSISTENT_BASIC.builder().headers(Map[String, AnyRef]("bar" -> expected.bar.value).asJava).build(),
        Payload.from(expected.baz.value))

      publisher(publishCommand).futureValue

      handler.receivedMessages should have size 1
      handler.receivedMessages.head shouldBe expected
    }
  }

  ignore("DeadLetter upon exception from handler") {
    val queueName = QueueName("exception-from-handler" + Random.nextInt())
    val handler = new StubConsumeHandler[Delivery]()
    val dlqHandler = new QueueWatcher[Delivery]
    for {
      amqpClient <- AmqpClientConfig("33.33.33.11", 5672, "guest", "guest", None)
      declarations = requeueDeclarations(queueName) collect {
        case ex: Exchange => ex.autoDelete.expires(1.minute)
        case q: Queue => q.autoDelete.expires(1.minute)
      }
      _ <- DeclarationLifecycle(declarations, amqpClient)
      publisher <- amqpClient.publisher()

      consumer <- amqpClient.consumer(queueName, handler)

      _ <- amqpClient.consumer(QueueName(s"${queueName.value}.dlq"), dlqHandler)
    } {

      dlqHandler.receivedMessages shouldBe 'empty
      handler.nextException = Some(new RuntimeException("Hello, world"))

      val expectedMessage = "Message to dlq"
      publisher(PublishCommand(ExchangeName(""), RoutingKey(queueName.value), MessageProperties.MINIMAL_PERSISTENT_BASIC, Payload.from(expectedMessage)))

      eventually {
        handler.receivedMessages should have size 1
        dlqHandler.receivedMessages should have size 1
      }
    }
  }


  test("Can consume messages from a (pre-existing) queue with the raw consumer") {
    val handler = new StubConsumeHandler[Delivery]()
    Lifecycle.using(rawConsumer(QueueName("bucky-consumer-raw-test"), handler)) {
      publisher =>
        handler.receivedMessages shouldBe 'empty

        val expectedMessage = "Hello World!"
        publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-raw-test"), MessageProperties.MINIMAL_PERSISTENT_BASIC, Payload.from(expectedMessage)))

        eventually {
          handler.receivedMessages should have size 1
          inside(handler.receivedMessages.head) {
            case Delivery(body, _, _, _) => Payload(body.value).unmarshal[String] shouldBe Success(expectedMessage)
          }
        }
    }
  }

  test("Message headers are exposed to (raw) consumers") {
    val handler = new StubConsumeHandler[Delivery]()
    Lifecycle.using(rawConsumer(QueueName("bucky-consumer-headers-test"), handler)) {
      publisher =>
        handler.receivedMessages shouldBe 'empty

        val expectedMessage = "Hello World!"

        import scala.collection.convert.wrapAll._

        val messageProperties = new BasicProperties.Builder()
          .contentType(MessageProperties.PERSISTENT_BASIC.getContentType)
          .deliveryMode(2)
          .priority(0)
          .headers(mapAsJavaMap(Map("hello" -> "world")))
          .build()

        publisher(PublishCommand(ExchangeName(""), RoutingKey("bucky-consumer-headers-test"), messageProperties, Payload.from(expectedMessage)))

        eventually {
          handler.receivedMessages should have size 1
          inside(handler.receivedMessages.head) {
            case Delivery(body, _, _, properties) => properties.getHeaders.asScala.get("hello").map(_.toString) shouldBe Some("world")
          }
        }
    }
  }

  def messageConsumer[T](queueName: QueueName, handler: Handler[T], unmarshaller: DeliveryUnmarshaller[T]) = for {
    result <- base(defaultDeclaration(queueName))
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, AmqpClient.deliveryHandlerOf(handler, unmarshaller))
  } yield publisher


}
