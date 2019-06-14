### Getting Started

In order to get started with bucky, add the following to you `build.sbt`:

 
```scala 
val buckyVersion = "2.0.0-M10"
libraryDependencies ++= Seq(
    "com.itv"                    %% "bucky-core"              % buckyVersion,
    "com.itv"                    %% "bucky-circe"             % buckyVersion,            //for circe based marshallers/unmarshallers
    "com.itv"                    %% "bucky-argonaut"          % buckyVersion,            //for argonaut based marhsallers/unmarshallers
    "com.itv"                    %% "bucky-test"              % buckyVersion % "test,it" //optional
    "com.itv"                    %% "bucky-kamon"             % buckyVersion,            //optional
)
```

or for ammonite:
```scala
import $ivy.`com.itv::bucky-core:2.0.0-M10` 
import $ivy.`com.itv::bucky-circe:2.0.0-M10`
```

Imports, implicits and config:
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
```
Registering a simple consumer:
```scala
object MyApp extends IOApp {
  case class Message(foo: String)

  val config = AmqpClientConfig(host = "127.0.0.1", port = 5672, username = "guest", password = "guest")
  val declarations = List(
    Queue(QueueName("queue-name")),
    Exchange(ExchangeName("exchange-name")).binding(RoutingKey("rk") -> QueueName("queue-name"))
  )

  class MyHandler extends Handler[IO, Message] {
    override def apply(m: Message): IO[ConsumeAction] =
      IO(Ack)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    (for {
      client <- AmqpClient[IO](config)
      handler = new MyHandler
      _ <- client.declareR(declarations)
      _ <- client.registerConsumerOf(QueueName("queue-name"), handler)
    } yield ()).use(_ => IO.never)
  }
}
```

Publishing a message:
```scala
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
      val publisher = client.publisherOf[Message](ExchangeName("exchange-name"), RoutingKey("rk"))
      publisher(Message("Hello"))
    } *> IO(ExitCode.Success)
  }
}
```

For easiness of use, bucky supports the creation of [Wirings](./wiring). A [Wiring](./wiring) centralizes the definition
of both ends of the communication (consumer/publisher) as well as the declarations of queues and exchanges
in a single place. 
