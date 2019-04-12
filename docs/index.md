### Overview
Bucky is a scala library developed at ITV which can be used to publish messages to a RabbitMQ broker as well as to consume messages.
  
The library is built on top of the [java amqp client](https://github.com/rabbitmq/rabbitmq-java-client).

Current version of bucky is based on:
 
| Project     | Version  |
|:------------|:--------:|
| cats        | 1.6.0    |
| cats-effects| 1.2.0    |
| amqp-client | 5.6.0    |


### Getting Started

In order to get started with bucky, add the following to you `build.sbt`:
 
```scala 
val buckyVersion = "2.0.0-M4"
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
import $ivy.`com.itv::bucky-core:2.0.0-M4` 
import $ivy.`com.itv::bucky-circe:2.0.0-M4`
```

Imports, implicits and config:
```scala 
import cats._
import cats.implicits._
import cats.effect._
import io.circe.generic.auto._

import com.itv.bucky.decl.Exchange
import com.itv.bucky.decl.Queue
import com.itv.bucky._
import com.itv.bucky.circe._
import com.itv.bucky.consume._


implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(10))
implicit val cs : ContextShift[IO] = IO.contextShift(ec)
implicit val timer: Timer[IO] = IO.timer(ec)

val config = AmqpClientConfig(host = "127.0.0.1", port= 5672, username="guest", password="guest")
case class Message(foo: String)

```
Registering a consumer:
```scala
val clientResource = AmqpClient[IO](config)
def handler(s: Message) : IO[ConsumeAction] = {
  for {
    _ <- IO.delay(println(s"Received: $s"))
  } yield Ack
}
clientResource.use { client =>
   val declarations = List(
    Queue(QueueName("queue-name")),
    Exchange(ExchangeName("exchange-name")).binding(RoutingKey("rk") -> QueueName("queue-name"))
   )
   
   for {
    _ <- client.declare(declarations)
    _ <- client.registerConsumerOf[Message](QueueName("queue-name"), handler)
    _ <- IO.never //keep running the consumer (for demo purposes only)
   } yield ()
  }.unsafeRunAsync(println)
```

Publishing a message:
```scala
val clientResource = AmqpClient[IO](config)
clientResource.use { client =>
   val publisher = client.publisherOf[Message](ExchangeName("exchange-name"), RoutingKey("rk"))
   for {
    _ <- client.declare(List(Exchange(ExchangeName("exchange-name"))))
    _ <- publisher(Message("Hello"))
   } yield "Message Published"
  }.unsafeRunAsync(println)
```

For easiness of use, bucky supports the creation of [Wirings](./wiring). A [Wiring](./wiring) centralizes the definition
of both ends of the communication (consumer/publisher) as well as the declarations of queues and exchanges
in a single place. 
