package itv.bucky

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.MessageProperties
import itv.utils.BlobMarshaller

object BlobSerializer {

  def blobSerializer[T](implicit marshaller: BlobMarshaller[T]) = BlobSerializer[T](marshaller)

  case class BlobSerializer[T](marshaller: BlobMarshaller[T], properties: Option[BasicProperties] = None) {

    def using(routingKey: RoutingKey): BlobSerializerWithoutExchange[T] =
      BlobSerializerWithoutExchange(routingKey, properties, marshaller)

    def using(exchange: ExchangeName): BlobSerializerWithoutRoutingKey[T] =
      BlobSerializerWithoutRoutingKey(exchange, properties, marshaller)

    def using(basicProperties: BasicProperties): BlobSerializer[T] =
      copy(properties = Some(basicProperties))

  }

  case class BlobSerializerWithoutRoutingKey[T](exchange: ExchangeName, properties: Option[BasicProperties] = None, marshaller: BlobMarshaller[T]) {

    def using(routingKey: RoutingKey): BlobSerializerBuilder[T] =
      BlobSerializerBuilder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: BasicProperties): BlobSerializerWithoutRoutingKey[T] =
      copy(properties = Some(basicProperties))

  }

  case class BlobSerializerWithoutExchange[T](routingKey: RoutingKey, properties: Option[BasicProperties] = None, marshaller: BlobMarshaller[T]) {

    def using(exchange: ExchangeName): BlobSerializerBuilder[T] =
      BlobSerializerBuilder(exchange, routingKey, properties, marshaller)

    def using(basicProperties: BasicProperties): BlobSerializerWithoutExchange[T] =
      copy(properties = Some(basicProperties))
  }

  case class BlobSerializerBuilder[T](exchange: ExchangeName, routingKey: RoutingKey, properties: Option[BasicProperties], marshaller: BlobMarshaller[T]) extends PublishCommandSerializer[T] {

    override def toPublishCommand(t: T): PublishCommand =
      PublishCommand(exchange, routingKey, properties.getOrElse(MessageProperties.PERSISTENT_BASIC), marshaller.toBlob(t))

    def using(basicProperties: BasicProperties): BlobSerializerBuilder[T] =
      copy(properties = Some(basicProperties))

  }

}