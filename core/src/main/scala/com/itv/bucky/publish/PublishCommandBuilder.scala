package com.itv.bucky.publish

import com.itv.bucky.{ExchangeName, PayloadMarshaller, RoutingKey}

trait PublishCommandBuilder[T] {
  def toPublishCommand(t: T): PublishCommand
}

object PublishCommandBuilder {

  def publishCommandBuilder[T](marshaller: PayloadMarshaller[T]) = NothingSet[T](marshaller)

  case class NothingSet[T](marshaller: PayloadMarshaller[T],
                           mandatory: Boolean = false,
                           properties: Option[MessageProperties] = None) {

    def using(routingKey: RoutingKey): WithoutExchange[T] =
      WithoutExchange(routingKey, properties, mandatory, marshaller)

    def using(exchange: ExchangeName): WithoutRoutingKey[T] =
      WithoutRoutingKey(exchange, properties, mandatory, marshaller)

    def using(basicProperties: MessageProperties): NothingSet[T] =
      copy(properties = Some(basicProperties))

    def using(mandatory: Boolean): NothingSet[T] =
      copy(mandatory = mandatory)

  }

  case class WithoutRoutingKey[T](exchange: ExchangeName,
                                  properties: Option[MessageProperties] = None,
                                  mandatory: Boolean = false,
                                  marshaller: PayloadMarshaller[T]) {

    def using(routingKey: RoutingKey): Builder[T] =
      Builder(exchange, routingKey, properties, mandatory, marshaller)

    def using(basicProperties: MessageProperties): WithoutRoutingKey[T] =
      copy(properties = Some(basicProperties))

    def using(mandatory: Boolean): WithoutRoutingKey[T] =
      copy(mandatory = mandatory)

  }

  case class WithoutExchange[T](routingKey: RoutingKey,
                                properties: Option[MessageProperties] = None,
                                mandatory: Boolean = false,
                                marshaller: PayloadMarshaller[T]) {

    def using(exchange: ExchangeName): Builder[T] =
      Builder(exchange, routingKey, properties, mandatory, marshaller)

    def using(basicProperties: MessageProperties): WithoutExchange[T] =
      copy(properties = Some(basicProperties))

    def using(mandatory: Boolean): WithoutExchange[T] =
      copy(mandatory = mandatory)
  }

  case class Builder[T](exchange: ExchangeName,
                        routingKey: RoutingKey,
                        properties: Option[MessageProperties],
                        mandatory: Boolean,
                        marshaller: PayloadMarshaller[T])
      extends PublishCommandBuilder[T] {

    override def toPublishCommand(t: T): PublishCommand =
      PublishCommand(exchange, routingKey, properties.fold(MessageProperties.persistentBasic)(identity), marshaller(t), mandatory)

    def using(basicProperties: MessageProperties): Builder[T] =
      copy(properties = Some(basicProperties))

  }

}
