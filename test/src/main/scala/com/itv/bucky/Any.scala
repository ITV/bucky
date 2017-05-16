package com.itv.bucky

import scala.language.higherKinds
import scala.util.Random

object Any {

  def publishCommand(exchangeName: ExchangeName = ExchangeName("exchange"),
                     routingKey: RoutingKey = RoutingKey("routing.key"),
                     messageProperties: MessageProperties = MessageProperties.persistentBasic,
                     payload: Payload = Any.payload()) =
    PublishCommand(exchangeName, routingKey, messageProperties, payload)

  def payload() =
    Payload.from(string())


  def string() =
    s"Hello World ${new Random().nextInt(10000)}! "


  def queue() =
    QueueName(s"bucky-queue-${new Random().nextInt(10000)}")
}

object HeaderExt {
  def apply(header: String, properties: MessageProperties): Option[String] =
    properties.headers.get(header).map(_.toString)
}


