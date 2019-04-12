## Serialization

When publishing and consuming messages, Bucky needs to know how to serialize
Scala data structures into something RabbitMQ understands. For this we use
`Marshaller` and `Unmarshaller` classes.

#### Automatic derivation

Bucky offers automatic derivation of both marshallers and unmarshallers when
using JSON and [Circe](https://circe.github.io/circe/) for the serialization library.

We strongly recommend using automatic derivation of marshallers and unmarshallers
for all messages sent across RabbitMQ.

To make use of automatic derivation, you need to import all Circe dependencies:

```scala
import com.itv.bucky.circe._
import io.circe.generic.auto._
```

Once you have imported the dependencies, you can create a publisher or consumer
for any of your case classes:

```scala

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.circe._
import io.circe.generic.auto._

object AutomaticDerivation {

  case class Person(name: String, age: Int)

  def publisher(client: AmqpClient[IO]) =
    for {
      // Create a publisher
      publisher <- IO.pure(client.publisherOf[Person](ExchangeName("persons"), RoutingKey("persons")))
      // Send a message
      _ <- publisher(Person("Alice", 22))
    } yield ()
}


```

#### Custom marshallers

If you wish to not use automatic derivation of JSON codecs for messages, you
are free to implement your own serialization logic. Please check out the
[Circe](https://circe.github.io/circe/) documentation for detailed instructions.

Once you have created custom serializers, you can use them together with Bucky:

```scala
import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.circe._
import io.circe.Encoder
import io.circe.generic.semiauto._

object CustomMarshaller {

  case class Name(value: String)
  object Name {
    implicit val encoder = Encoder.encodeString.contramap[Name](_.value)
  }

  case class Age(value: Int)
  object Age {
    implicit val encoder = Encoder.encodeInt.contramap[Age](_.value)
  }

  case class Person(name: Name, age: Age)
  object Person {
    implicit val PersonEncoder: Encoder[Person] = deriveEncoder[Person]
  }

  def publisher(client: AmqpClient[IO]) =
    for {
      // Create a publisher
      publisher <- IO.pure(client.publisherOf[Person](ExchangeName("persons"), RoutingKey("persons")))
      // Send a message
      _ <- publisher(Person(Name("Alice"), Age(22)))
    } yield ()
}

```


#### XML serialization

Bucky offers out-of-the-box support for serializing to and from XML.

To make use of the XML serializer, import from `XmlSupport`:

```scala

import cats.effect.IO
import com.itv.bucky._

import scala.xml.Elem

class XmlMarshalledPublisher {
  implicit val marshaller = XmlSupport.marshallerFromElem

  def publisher(client: AmqpClient[IO]) = {
    for {
      publisher <- IO.pure(client.publisherOf[Elem](ExchangeName("persons"), RoutingKey("persons")))
      _ <- publisher(<message><person><name>Alice</name><age>22</age></person></message>)
    } yield ()
  }
}

```
