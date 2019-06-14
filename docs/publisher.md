## Publishers

Publishers can be used to send messages to a rabbit Broker. The creation of a publisher,
provided you have a `AmqpClient[_]` and the correct encoders/decoders, is quite trivial.

Defining a publisher
```scala
  import cats._
  import cats.implicits._
  import cats.effect._
  import io.circe.generic.auto._
  import scala.concurrent.{ExecutionContext}
  import com.itv.bucky.decl.Exchange
  import com.itv.bucky.decl.Queue
  import com.itv.bucky._
  import com.itv.bucky.circe._
  import com.itv.bucky.consume._
  import com.itv.bucky.publish._
  
  object MyApp extends IOApp {
    case class Message(foo: String)
  
    val config = AmqpClientConfig(host = "127.0.0.1", port = 5672, username = "guest", password = "guest")
    val declarations = List(
      Queue(QueueName("queue-name")),
      Exchange(ExchangeName("exchange-name")).binding(RoutingKey("rk") -> QueueName("queue-name"))
    )
  
    override def run(args: List[String]): IO[ExitCode] = {
      implicit val ec: ExecutionContext = ExecutionContext.global
      (for {
        client <- AmqpClient[IO](config)
        _ <- client.declareR(declarations)
      } yield client).use { client =>
        val publisher = client.publisherOf[Message](ExchangeName("exchange-name"), RoutingKey("rk"))  // <- publisher being created
        publisher(Message("Hello")) //message is sent 
      } *> IO(ExitCode.Success)
    }
  }
```

In order to send a message to the broker, all that's needed is to call the `apply` method with
the desired message on the publisher:

```scala
  val publisher = client.publisherOf[AMessageType](ExchangeName("exchange-name"), RoutingKey("rk"))
  publisher(aMessage) //aMessage will be sent 
```
Note: publishers can be reused, there's no real need to create a new one per message

### Headers and Message Properties
In order to have more control over the headers and message properties sent to the broker,
on can use a `PublishCommandBuilder` to customise all those properties as follows:

```scala

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.publish._
import io.circe.generic.auto._
import io.circe.syntax._

object CustomPublishBuilder {
  case class Person(name: String, age: Int)

  val marshaller = new PayloadMarshaller[Person] {
    def apply(person: Person): Payload =
      Payload(person.asJson.noSpaces.getBytes())
  }

  def builder: PublishCommandBuilder.Builder[Person] =
    publishCommandBuilder[Person](marshaller)
      .using(ExchangeName("person-exchange"))
      .using(RoutingKey("person-route"))

  def publisher(client: AmqpClient[IO]) =
    for {
      publish <- IO.delay(client.publisherOf(builder))
      _       <- publish(Person("Alice", 22))
    } yield ()
}

```

