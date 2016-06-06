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

  case class Exchange(name: ExchangeName,
                      autoDelete: Boolean,
                      durable: Boolean,
                      internal: Boolean,
                      arguments: Map[String, AnyRef]) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.exchangeDeclare(name.value, "direct", autoDelete, durable, internal, arguments.toMap.asJava))

  }

  case class Binding(exchangeName: ExchangeName,
                     queueName: QueueName,
                     routingKey: RoutingKey,
                     arguments: Map[String, AnyRef]) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.queueBind(queueName.value, exchangeName.value, routingKey.value, arguments.toMap.asJava))

  }

  case class Queue(queueName: QueueName,
                   autoDelete: Boolean,
                   durable: Boolean,
                   internal: Boolean,
                   arguments: Map[String, AnyRef]) extends Declaration {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, _.queueDeclare(queueName.value, autoDelete, durable, internal, arguments.toMap.asJava))

  }

}
