



#### Testing handlers

##### Unit testing

When testing handlers, they can be treated similar to pure functions. 
They receive a message and produce an output in form of a consume action.

```scala
import cats.effect.IO
import com.itv.bucky.consume.Ack
import com.itv.bucky.{Handler, consume}
import org.scalatest.{Matchers, WordSpec}

class ConsumerSpec extends WordSpec with Matchers {

  case class User(id: String)
  case class EmailAddress(value: String)
  case class Message(email: EmailAddress)

  class SendPasswordReset(
      fetchEmailAddress: User => IO[EmailAddress],
      sendMail: Message => IO[Unit]
  ) extends Handler[IO, User] {

    def apply(recipient: User): IO[consume.ConsumeAction] =
      for {
        emailAddress <- fetchEmailAddress(recipient)
        _            <- sendMail(Message(emailAddress))
      } yield Ack
  }

  "SendPasswordReset" should {
    "send emails to the user in question" in new Setup {
      result shouldBe Right(Ack)
    }
    "error out if fetching the email fails" in new Setup {
      override val emailResult = IO.raiseError(new RuntimeException("DB failed"))
      result.left.map(_.getMessage) shouldBe Left("DB failed")
    }
    "error out if sending the email fails" in new Setup {
      override val sendResult = IO.raiseError(new RuntimeException("Email failed"))
      result.left.map(_.getMessage) shouldBe Left("Email failed")
    }

  }

  trait Setup {
    val emailResult = IO.pure(EmailAddress("test@example.com"))
    val sendResult  = IO.unit

    lazy val handler = new SendPasswordReset(
      fetchEmailAddress = _ => emailResult,
      sendMail = _ => sendResult
    )

    lazy val result = handler(User("1")).attempt.unsafeRunSync()
  }
}

```  

##### Integration testing 

We can also test handlers by using an actual AMQP client and wiring up the handler 
function with a consumer.

```scala 

import cats.effect.IO
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.consume.Delivery
import com.itv.bucky.decl.{Exchange, Queue}
import com.itv.bucky.publish.PublishCommandBuilder
import com.itv.bucky.test.{IOAmqpClientTest, StubHandlers}
import com.itv.bucky.{ExchangeName, QueueName, RoutingKey}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

class ConsumerTest
  extends FunSuite
    with IOAmqpClientTest
    with ScalaFutures {

  test("Consuming messages should work") {
    runAmqpTest { client =>
      val exchange = ExchangeName("email")
      val queue    = QueueName("email")
      val rk       = RoutingKey("email")
      val message  = "Hello"
  
      val commandBuilder = PublishCommandBuilder
        .publishCommandBuilder[String](StringPayloadMarshaller)
        .using(exchange)
        .using(rk)

      val handler      = StubHandlers.ackHandler[IO, Delivery]
      val declarations = List(Queue(queue), Exchange(exchange).binding((rk, queue)))

      for {
      _ <- client.declare(declarations)
      _ <- client.registerConsumer(queue, handler)
      _ <- client.publisher()(commandBuilder.toPublishCommand("Test"))
      } yield {
        handler.receivedMessages should have size 1
      }
    }
  }
}


```