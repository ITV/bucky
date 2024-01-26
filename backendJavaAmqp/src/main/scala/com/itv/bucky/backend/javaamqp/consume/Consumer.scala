package com.itv.bucky.backend.javaamqp.consume

import com.itv.bucky.Payload
import com.itv.bucky.backend.javaamqp.MessagePropertiesConverters
import com.itv.bucky.consume.{ConsumerTag, Delivery}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.typesafe.scalalogging.StrictLogging

object Consumer extends StrictLogging {

  def deliveryFrom(tag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) =
    Delivery(Payload(body), ConsumerTag(tag), MessagePropertiesConverters(envelope), MessagePropertiesConverters(properties))

}
