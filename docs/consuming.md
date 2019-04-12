## Consuming messages

Consuming messages with Bucky is done by creating a consumer that executes 
a handler function every time a message arrives. 

```scala 
import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.circe._
import com.itv.bucky.consume._
import com.itv.bucky.decl.Queue
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._

object ConsumeMessages extends StrictLogging {
  case class Person(name: String, age: Int)

  val queue = Queue(QueueName("people-messages"))

  def handler: Handler[IO, Person] = { message =>
    IO.delay {
      logger.info(s"${message.name} is ${message.age} years old")
      Ack
    }
  }

  def consume(client: AmqpClient[IO]) = {
    for {
    _ <- client.declare(List(queue))
    _ <- client.registerConsumerOf[Person](queue.name, handler)
    } yield ()
  }

}
```

#### Re-queuing messages on errors

In most cases you will want to handle failures in your handler by re-queuing
the message that you were processing. Bucky gives you tools to create the 
re-queueing behaviour out of the box. You can configure both how many times you 
want a message to be re-queued as well as the delay between messages.

```scala

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.consume._
import com.itv.bucky.circe._
import com.itv.bucky.decl.Queue
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.typesafe.scalalogging.StrictLogging
import concurrent.duration._
import io.circe.generic.auto._

object RequeueConsumeMessages extends StrictLogging {
  case class Person(name: String, age: Int)

  val queue = Queue(QueueName("people-messages"))

  def handler: RequeueHandler[IO, Person] = { message =>
    for {
      _ <- IO.raiseError(new RuntimeException("Throwing intentional error"))
    } yield Ack
  }

  def consume(client: AmqpClient[IO]) = {
    for {
      _ <- client.declare(List(queue))
      _ <- client.registerRequeueConsumerOf[Person](
        queueName = queue.name,
        handler = handler,
        requeuePolicy = RequeuePolicy(maximumProcessAttempts = 10,requeueAfter = 5.minutes)
      )
    } yield ()
  }
}

```

In the above example the `handler` function will error out every time it runs. 
When this happens, the consumer is configured to requeue the message that lead 
to the error up to 10 times in a 5 minute interval.

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



