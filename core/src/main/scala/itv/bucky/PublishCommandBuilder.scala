package itv.bucky


trait PublishCommandBuilder[T] {
  def toPublishCommand(t: T): PublishCommand
}

object PublishCommandBuilder {

  def publishCommandBuilder[T](marshaller: PayloadMarshaller[T]) = NothingSet[T](marshaller)

  case class NothingSet[T](marshaller: PayloadMarshaller[T], properties: Option[AmqpProperties] = None) {

    def using(routingKey: RoutingKey): WithoutExchange[T] =
      WithoutExchange(routingKey, properties, marshaller)

    def using(exchange: ExchangeName): WithoutRoutingKey[T] =
      WithoutRoutingKey(exchange, properties, marshaller)

    def using(basicProperties: AmqpProperties): NothingSet[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutRoutingKey[T](exchange: ExchangeName, properties: Option[AmqpProperties] = None, marshaller: PayloadMarshaller[T]) {

    def using(routingKey: RoutingKey): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: AmqpProperties): WithoutRoutingKey[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutExchange[T](routingKey: RoutingKey, properties: Option[AmqpProperties] = None, marshaller: PayloadMarshaller[T]) {

    def using(exchange: ExchangeName): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: AmqpProperties): WithoutExchange[T] =
      copy(properties = Some(basicProperties))
  }

  case class Builder[T](exchange: ExchangeName, routingKey: RoutingKey, properties: Option[AmqpProperties], marshaller: PayloadMarshaller[T]) extends PublishCommandBuilder[T] {

    override def toPublishCommand(t: T): PublishCommand =
      PublishCommand(exchange, routingKey, properties.fold(AmqpProperties())(identity), marshaller(t))

    def using(basicProperties: AmqpProperties): Builder[T] =
      copy(properties = Some(basicProperties))

  }

}