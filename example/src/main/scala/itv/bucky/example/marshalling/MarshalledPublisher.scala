package itv.bucky.example.marshalling

import com.itv.lifecycle.Lifecycle
import scala.concurrent.ExecutionContext.Implicits.global
import itv.bucky.PublishCommandBuilder.publishCommandBuilder
import itv.bucky._
import itv.bucky.decl.{DeclarationLifecycle, Exchange, Queue}
import itv.bucky.example.marshalling.Shared.Person

import scala.concurrent.duration._
import scala.concurrent._

object MarshalledPublisher extends App {

  object Declarations {
    val queue = Queue(QueueName("queue.people"))
    val routingKey = RoutingKey("personPublisherRoutingKey")
    val exchange = Exchange(ExchangeName("exchange.person-publisher")).binding(routingKey -> queue.queueName)

    val all = List(queue, exchange)
  }

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  /**
    * A publisher delivers a message of a fixed type to an exchange.
    * The routing key (along with exchange configuration) determine where the message will reach.
    */
  val publisherConfig =
    publishCommandBuilder(Shared.personMarshaller) using Declarations.routingKey using Declarations.exchange.name

  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Publisher[Person]] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      publisher <- amqpClient.publisherOf(publisherConfig)
    }
      yield publisher

  Lifecycle.using(lifecycle) { publisher: Publisher[Person] =>
    val result: Future[Unit] = publisher(Person("Bob", 21))
    Await.result(result, 1.second)
  }

}
