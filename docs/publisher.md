## Publishers

Publishers can be used to send messages to a rabbit Broker. The creation of a publisher,
provided you have a `AmqpClient[_]` and the correct encoders/decoders, is quite trivial.

Defining a publisher
```scala
def foo(client: AmqpClient[IO]) = {
  val publisher = client.publisherOf[AMessageType](ExchangeName("exchange-name"), RoutingKey("rk")) 
}
```

In order to send a message to the broker, all that's needed is to call the `apply` method with
the desired message on the publisher:

```scala
def foo(client: AmqpClient[IO]) = {
  val publisher = client.publisherOf[AMessageType](ExchangeName("exchange-name"), RoutingKey("rk"))
  publisher(aMessage) //aMessage will be sent 
}
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

