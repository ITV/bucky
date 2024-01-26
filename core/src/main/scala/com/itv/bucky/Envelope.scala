package com.itv.bucky

case class Envelope(deliveryTag: Long, redeliver: Boolean, exchangeName: ExchangeName, routingKey: RoutingKey)
