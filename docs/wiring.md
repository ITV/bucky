## Wiring

Bucky offers a wiring API to create publishers and consumers of messages
succinctly and with little effort.

### Usage

To get started with creating publishers and consumers for specific tasks,
start by importing everything related to automatic derivation of JSON codecs as
well as everything related to wiring:

```scala

import com.itv.bucky.circe._
import com.itv.bucky.wiring._
import io.circe.generic.auto._

```

Once you have imported all dependencies, you can proceed by
creating wiring declarations. Wiring declarations expect at least the type of
the messages exchanged over RabbitMQ as well as a name for the wiring.

```scala

case class EmailParams(userId: String, emailAddress: String)

object SendPasswordReset extends Wiring[EmailParams](
 name = WiringName("tasks.password-reset-email")
)

```

Once you have declared a wiring, you can start using it by creating publishers
and consumers. When you create publishers and consumers, the wiring will declare
all relevant exchanges, routing keys and queues for you.

#### Creating a consumer

```scala

def startConsumer(client: AmqpClient[IO]) =
  for {
    _ <- SendPasswordReset.registerConsumer(client) { message =>
      IO.delay {
        println("Sending password reset email: destination=${message.emailAddress}")
        Ack
      }
    }
  } yield ()

```

#### Creating a publisher

```scala

def startPublisher(client: AmqpClient[IO]) =
  for {
    sendPasswordReset <- SendPasswordReset.publisher(client)
    _ <- sendPasswordReset(EmailParams("123", "test@example.com"))
  } yield ()

```

#### Additional options

The previous example of a `SendPasswordReset` wiring was using the default
options for declaration. You can customise the wiring if you wish to not use
generated exchange names, routing keys, etc.

```scala

 object SendPasswordReset
     extends Wiring[EmailParams](
       WiringName("tasks.email.reminder"),
       setExchangeName = Some(ExchangeName("emails.outgoing")),
       setRoutingKey = Some(RoutingKey("emails.password.reset")),
       setQueueName = Some(QueueName("emails.password.reset")),
       setExchangeType = Some(Topic),
       setRequeuePolicy = Some(RequeuePolicy(maximumProcessAttempts = 10, requeueAfter = 10.minutes)),
       setPrefetchCount = Some(10)
     )

```

#### Sending messages with custom headers

You can also create publishers that send custom headers with every message as follows:

```scala

def startPublisherWithHeaders(client: AmqpClient[IO]) =
   for {
     sendPasswordReset <- SendPasswordReset.publisherWithHeaders(client)
     _ <- sendPasswordReset(
       EmailParams("123", "test@example.com"),
       Map("x-sent-timestamp" -> ZonedDateTime.now(ZoneId.of("UTC")))
     )
   } yield ()

```
