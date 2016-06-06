package itv.bucky

import com.rabbitmq.client.Channel

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

package object decl {

  sealed trait Declaration {
    def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit]
  }

  object Declaration {
    def applyAll(declaration: Iterable[Declaration], client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      Future.sequence(declaration.map(_.applyTo(client))).map(_ => ())
  }

  private def declOperation(client: AmqpClient, thunk: Channel => Unit)(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      client withChannel thunk
    }.map(_ => ())

  sealed trait ExchangeType {
    def value: String
  }
  case object Direct extends ExchangeType {
    override def value: String = "direct"
  }

  case class Exchange(name: ExchangeName,
                      exchangeType: ExchangeType = Direct,
                      durable: Boolean = true,
                      autoDelete: Boolean = false,
                      internal: Boolean = false,
                      arguments: Map[String, AnyRef] = Map.empty) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.exchangeDeclare(name.value, exchangeType.value, durable, autoDelete, internal, arguments.toMap.asJava))

  }

  case class Binding(exchangeName: ExchangeName,
                     queueName: QueueName,
                     routingKey: RoutingKey,
                     arguments: Map[String, AnyRef]) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.queueBind(queueName.value, exchangeName.value, routingKey.value, arguments.toMap.asJava))

  }

  case class Queue(queueName: QueueName,
                   durable: Boolean = true,
                   exclusive: Boolean = false,
                   autoDelete: Boolean = false,
                   arguments: Map[String, AnyRef] = Map.empty) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.queueDeclare(queueName.value, exclusive, exclusive, autoDelete, arguments.toMap.asJava))

  }

}
