package itv.bucky

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.MessageProperties

trait PublishCommandBuilder[T] {
  def toPublishCommand(t: T): PublishCommand
}

object PublishCommandBuilder {

  def publishCommandBuilder[T](implicit marshaller: PayloadMarshaller[T]) = NothingSet[T](marshaller)

  case class NothingSet[T](marshaller: PayloadMarshaller[T], properties: Option[BasicProperties] = None) {

    def using(routingKey: RoutingKey): WithoutExchange[T] =
      WithoutExchange(routingKey, properties, marshaller)

    def using(exchange: ExchangeName): WithoutRoutingKey[T] =
      WithoutRoutingKey(exchange, properties, marshaller)

    def using(basicProperties: BasicProperties): NothingSet[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutRoutingKey[T](exchange: ExchangeName, properties: Option[BasicProperties] = None, marshaller: PayloadMarshaller[T]) {

    def using(routingKey: RoutingKey): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: BasicProperties): WithoutRoutingKey[T] =
      copy(properties = Some(basicProperties))

  }

  case class WithoutExchange[T](routingKey: RoutingKey, properties: Option[BasicProperties] = None, marshaller: PayloadMarshaller[T]) {

    def using(exchange: ExchangeName): Builder[T] =
      Builder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: BasicProperties): WithoutExchange[T] =
      copy(properties = Some(basicProperties))
  }

  case class Builder[T](exchange: ExchangeName, routingKey: RoutingKey, properties: Option[BasicProperties], marshaller: PayloadMarshaller[T]) extends PublishCommandBuilder[T] {

    override def toPublishCommand(t: T): PublishCommand =
      PublishCommand(exchange, routingKey, properties.getOrElse(MessageProperties.PERSISTENT_BASIC), marshaller(t))

    def using(basicProperties: BasicProperties): Builder[T] =
      copy(properties = Some(basicProperties))

  }

}