package com.itv.bucky.backend.javaamqp

import com.itv.bucky.{Envelope, ExchangeName, RoutingKey}
import com.rabbitmq.client.{Envelope => RabbitEnvelope}

object EnvelopeConversion {
  def fromJavaEnvelope(envelope: RabbitEnvelope): Envelope =
    Envelope(envelope.getDeliveryTag, envelope.isRedeliver, ExchangeName(envelope.getExchange), RoutingKey(envelope.getRoutingKey))
}
