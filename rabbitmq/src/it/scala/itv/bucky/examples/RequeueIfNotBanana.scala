package itv.bucky.examples

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import itv.bucky._
import itv.bucky.pattern.requeue._
import itv.bucky.decl.DeclarationLifecycle
import itv.contentdelivery.lifecycle.Lifecycle
import itv.bucky.PublishCommandBuilder._
import itv.bucky.Unmarshaller._

import scala.concurrent.Future
import itv.bucky.SameThreadExecutionContext.implicitly
import org.joda.time.DateTime

import scala.concurrent.duration._
import argonaut._
import Argonaut._
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller

case class Fruit(name: String)

object Fruit {
  val deserializer: PayloadUnmarshaller[Fruit] = StringPayloadUnmarshaller.map(Fruit.apply)
}

case class DeliveryRequest(fruit: Fruit, timeOfRequest: DateTime)

object DeliveryRequest {
  val marshaller: PayloadMarshaller[DeliveryRequest] =
    StringPayloadMarshaller contramap { request =>
      jObjectFields(
        "name" -> jString(request.fruit.name),
        "timeOfRequest" -> jString(request.timeOfRequest.toString)
      ).nospaces
    }
}

case class RequeueIfNotBananaHandler(requestDelivery: Publisher[DeliveryRequest]) extends RequeueHandler[Fruit] with StrictLogging {

  override def apply(fruit: Fruit): Future[RequeueConsumeAction] =
    fruit match {
      case fruit@Fruit("Banana") => {
        logger.info("Fruit was Banana, cool")

        val deliveryRequest = DeliveryRequest(fruit, DateTime.now())
        requestDelivery(deliveryRequest).map(_ => Ack)
      }
      case _ => {
        logger.info(s"$fruit is not Banana, requeueing")
        Future.successful(Requeue)
      }
    }

}

case class RequeueIfNotBanana(clientLifecycle: Lifecycle[AmqpClient]) {

  val queueName = QueueName("requeue.consumer.example")

  val deliveryRequestPublishCommandBuilder =
    publishCommandBuilder(DeliveryRequest.marshaller) using ExchangeName("") using RoutingKey("fruit.delivery")

  val consumerLifecycle =
    for {
      client <- clientLifecycle
      _ <- DeclarationLifecycle(requeueDeclarations(queueName, retryAfter = 1.second), client)

      requestDelivery <- client.publisherOf(deliveryRequestPublishCommandBuilder)

      handler = RequeueIfNotBananaHandler(requestDelivery)
      _ <- client.requeueHandlerOf(queueName, handler, RequeuePolicy(maximumProcessAttempts = 3, requeueAfter = 10.seconds), Fruit.deserializer)
    }
      yield ()

}

object Main extends App {
  def readConfig(config: Config): AmqpClientConfig = {
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
  }

  RequeueIfNotBanana(readConfig(ConfigFactory.load("bucky"))).consumerLifecycle.runUntilJvmShutdown()
}
