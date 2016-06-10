package itv.bucky

import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration
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
                      isDurable: Boolean = true,
                      shouldAutoDelete: Boolean = false,
                      isInternal: Boolean = false,
                      arguments: Map[String, AnyRef] = Map.empty,
                      bindings: List[Binding] = List.empty) extends Declaration with StrictLogging {

    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] = {
      val createExchange =
       declOperation(client, {
         logger.info(s"Declaring Exchange($name, $exchangeType, isDurable=$isDurable, shouldAutoDelete=$shouldAutoDelete, isInternal=$isInternal, arguments=$arguments)")
         _.exchangeDeclare(name.value, exchangeType.value, isDurable, shouldAutoDelete, isInternal, arguments.asJava)
       })
      val createBindings: List[Future[Unit]] =
        bindings.map(_.applyTo(client))

      Future.sequence(createBindings :+ createExchange).map(_ => ())
    }

    def notDurable: Exchange = copy(isDurable = false)
    def autoDelete: Exchange = copy(shouldAutoDelete = true)
    def internal: Exchange = copy(isInternal = true)

    def argument(value: (String, AnyRef)): Exchange = copy(arguments = this.arguments + value)
    def expires(value: FiniteDuration): Exchange = argument("x-expires" -> Long.box(value.toMillis))

    def binding(routingKeyToQueue: (RoutingKey, QueueName), arguments: Map[String, AnyRef] = Map.empty): Exchange =
      copy(bindings = this.bindings :+ Binding(name, routingKeyToQueue._2, routingKeyToQueue._1, arguments))

  }

  case class Binding(exchangeName: ExchangeName,
                     queueName: QueueName,
                     routingKey: RoutingKey,
                     arguments: Map[String, AnyRef]) extends Declaration with StrictLogging {
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, {
        logger.info(s"Declaring $this")
        _.queueBind(queueName.value, exchangeName.value, routingKey.value, arguments.asJava)
      })

  }

  case class Queue(queueName: QueueName,
                   isDurable: Boolean = true,
                   isExclusive: Boolean = false,
                   shouldAutoDelete: Boolean = false,
                   arguments: Map[String, AnyRef] = Map.empty) extends Declaration with StrictLogging {
    
    override def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] =
      declOperation(client, {
        logger.info(s"Declaring $this")
        _.queueDeclare(queueName.value, isDurable, isExclusive, shouldAutoDelete, arguments.asJava)
      })

    def notDurable: Queue = copy(isDurable = false)
    def exclusive: Queue = copy(isExclusive = true)
    def autoDelete: Queue = copy(shouldAutoDelete = true)

    def argument(value: (String, AnyRef)): Queue = copy(arguments = this.arguments + value)
    def expires(value: FiniteDuration): Queue = argument("x-expires" -> Long.box(value.toMillis))

    def deadLetterExchange(exchangeName: ExchangeName): Queue = argument("x-dead-letter-exchange" -> exchangeName.value)

    def messageTTL(value: FiniteDuration): Queue = argument("x-message-ttl" -> Long.box(value.toMillis))

  }

}
