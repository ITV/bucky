
## Publishing 

### Customizing publishing

If you want to have more control over your publishers and how they publish,
feel free to use `PublishCommandBuilder` to customise publishing.

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

