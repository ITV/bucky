package itv.bucky.example.publisher

import com.itv.lifecycle._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky._
import itv.bucky.decl.{DeclarationLifecycle, Exchange, Queue}
import itv.bucky.PublishCommandBuilder._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * AMQP objects used in this project
  *
  * Queue: queue.simplepublishe
  *
  * Exchange: exchange.simple-publisher
  *   direct, with 1 binding:
  *     simplePublishRoutingKey -> queue.simplepublishe
  */
object Declarations {

  val queue = Queue(QueueName("queue.simplepublisher"))
  val routingKey = RoutingKey("simplePublishRoutingKey")
  val exchange = Exchange(ExchangeName("exchange.simple-publisher")).binding(routingKey -> queue.queueName)

  val all = List(queue, exchange)

}

case class SimplePublisherMessage(value: String)

object SimplePublisherMessage {

  val marshaller: PayloadMarshaller[SimplePublisherMessage] = StringPayloadMarshaller.contramap(_.value)

}

case class SimplePublisher(amqpClientConfig: AmqpClientConfig) {

  val publisherConfig: PublishCommandBuilder[SimplePublisherMessage] = publishCommandBuilder(SimplePublisherMessage.marshaller) using Declarations.routingKey using Declarations.exchange.name

  val lifecycle: Lifecycle[Publisher[SimplePublisherMessage]] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _ <- DeclarationLifecycle(Declarations.all, amqpClient)
      publisher <- amqpClient.publisherOf(publisherConfig)
    }
      yield publisher

}

object Main extends App {

  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig("33.33.33.11", 5672, "guest", "guest")

  Lifecycle.using(SimplePublisher(amqpClientConfig).lifecycle) { publisher =>
    val publishResult: Future[Unit] = publisher(SimplePublisherMessage("Hello, world!"))
    Await.result(publishResult, Duration.Inf)
  }

}