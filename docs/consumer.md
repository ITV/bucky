## Consuming messages


### Simple Handler
Consuming messages with Bucky is done in two stages. First  you create a handler that's going to be
executed whenever you receive a message:

```scala 
case class Message(foo: String)
class MyHandler extends Handler[IO, Message] {
override def apply(m: Message): IO[ConsumeAction] =
  IO(Ack)
}
```

and then you register it like so: 
```scala
object MyApp extends IOApp {
  val config = AmqpClientConfig(host = "127.0.0.1", port = 5672, username = "guest", password = "guest")
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

#### Re-queuing messages on errors

In most cases you will want to handle failures in your handler by re-queuing
the message that you were processing. Bucky gives you tools to create the 
re-queueing behaviour out of the box. You can configure both how many times you 
want a message to be re-queued as well as the delay between messages.

In order to do so, you have to create a requeue handler first:

```scala
case class Message(foo: String)
class MyHandler extends RequeueHandler[IO, Message] {
override def apply(m: Message): IO[RequeuConsumeAction] = IO(Ack)
}
```

And then register it:
```scala
object MyApp extends IOApp {
  case class Message(foo: String)

  val config = AmqpClientConfig(host = "127.0.0.1", port = 5672, username = "guest", password = "guest")
  val declarations = List(
    Queue(QueueName("queue-name")),
    Exchange(ExchangeName("exchange-name")).binding(RoutingKey("rk") -> QueueName("queue-name"))
  ) ++ requeueDeclarations(QueueName("queue-name"))

  class MyHandler extends RequeueHandler[IO, Message] {
    override def apply(m: Message): IO[RequeueConsumeAction] = IO(Ack)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    (for {
      client <- AmqpClient[IO](config)
      handler = new MyHandler
      _ <- client.declareR(declarations)
      _ <- client.registerRequeueConsumerOf(QueueName("queue-name"), handler, RequeuePolicy(10, 3.seconds))
    } yield client).use{_ => IO.never}
  }
}
```

In the above example the `handler` function will error out every time it runs. 
When this happens, the consumer is configured to requeue the message that lead 
to the error up to 10 times in a 3 seconds interval.

