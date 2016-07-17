package itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

package object decl {

  sealed trait Declaration {
    def applyTo(client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit]
  }

  object Declaration {
    def applyAll(declaration: Iterable[Declaration], client: AmqpClient)(implicit ec: ExecutionContext): Future[Unit] = {
      val (bindings, rest) = declaration.partition {
        case binding: Binding => true
        case _ => false
      }
      for {
        _ <- Future.sequence(rest.map(_.applyTo(client))).map(_ => ())
        _ <- Future.sequence(bindings.map(_.applyTo(client))).map(_ => ())
      } yield ()
    }
  }

  private def declOperation(client: AmqpClient, thunk: AmqpOps => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] =
    client withDeclarations thunk

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
          _.declareExchange(this)
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
        _.bindQueue(this)
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
        _.declareQueue(this)
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
