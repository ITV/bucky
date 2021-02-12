package com.itv.bucky.consume

import com.itv.bucky.{MessagePropertiesConverters, Payload}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

object Consumer {

  def deliveryFrom(tag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) =
    Delivery(Payload(body), ConsumerTag(tag), MessagePropertiesConverters(envelope), MessagePropertiesConverters(properties))

}
