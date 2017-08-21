package com.itv.bucky

trait PublishCommandBuilder[T] {
  def toPublishCommand(t: T): PublishCommand
}

object PublishCommandBuilder {

  def publishCommandBuilder[T](marshaller: PayloadMarshaller[T]) = NothingSet[T](marshaller)

  case class NothingSet[T](marshaller: PayloadMarshaller[T], properties: Option[MessageProperties] = None) {

    def using(routingKey: RoutingKey): WithoutExchange[T] =
      WithoutExchange(routingKey, properties, marshaller)

    def using(exchange: ExchangeName): WithoutRoutingKey[T] =
      WithoutRoutingKey(exchange, properties, marshaller)

    def using(basicProperties: MessageProperties): NothingSet[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutRoutingKey[T](exchange: ExchangeName,
                                  properties: Option[MessageProperties] = None,
                                  marshaller: PayloadMarshaller[T]) {

    def using(routingKey: RoutingKey): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: MessageProperties): WithoutRoutingKey[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutExchange[T](routingKey: RoutingKey,
                                properties: Option[MessageProperties] = None,
                                marshaller: PayloadMarshaller[T]) {

    def using(exchange: ExchangeName): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: MessageProperties): WithoutExchange[T] =
      copy(properties = Some(basicProperties))
  }

  case class Builder[T](exchange: ExchangeName,
                        routingKey: RoutingKey,
                        properties: Option[MessageProperties],
                        marshaller: PayloadMarshaller[T])
      extends PublishCommandBuilder[T] {

    override def toPublishCommand(t: T): PublishCommand =
      PublishCommand(exchange, routingKey, properties.fold(MessageProperties.persistentBasic)(identity), marshaller(t))

    def using(basicProperties: MessageProperties): Builder[T] =
      copy(properties = Some(basicProperties))

  }

}
