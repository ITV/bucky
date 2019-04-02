package com.itv.bucky
import com.rabbitmq.client.{Envelope => RabbitEnvelope}

case class Envelope(deliveryTag: Long, redeliver: Boolean, exchangeName: ExchangeName, routingKey: RoutingKey)
object Envelope {
  def fromEnvelope(envelope: RabbitEnvelope) =
    Envelope(envelope.getDeliveryTag, envelope.isRedeliver, ExchangeName(envelope.getExchange), RoutingKey(envelope.getRoutingKey))
}
