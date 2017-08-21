package com.itv.bucky

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

package object decl {

  sealed trait Declaration {
    def run: AmqpOps => Try[Unit]
  }

  object Declaration extends StrictLogging {
    def runAll(declaration: Iterable[Declaration]): AmqpOps => Try[Unit] =
      ops => {
        val queues    = declaration.collect { case queue: Queue       => queue }
        val exchanges = declaration.collect { case exchange: Exchange => exchange }
        val bindings = declaration.collect {
          case binding: Binding                 => binding
          case exchangeBinding: ExchangeBinding => exchangeBinding
        }
        for {
          _ <- TryUtil.sequence(queues.map(_.run(ops)))
          _ <- TryUtil.sequence(exchanges.map(_.run(ops)))
          _ <- TryUtil.sequence(bindings.map(_.run(ops)))
        } yield ()
      }
  }

  sealed trait ExchangeType {
    def value: String
  }

  case object Direct extends ExchangeType {
    override def value: String = "direct"
  }
  case object Topic extends ExchangeType {
    override def value: String = "topic"
  }
  case object Fanout extends ExchangeType {
    override def value: String = "fanout"
  }
  case object Headers extends ExchangeType {
    override def value: String = "headers"
  }

  case class Exchange(name: ExchangeName,
                      exchangeType: ExchangeType = Direct,
                      isDurable: Boolean = true,
                      shouldAutoDelete: Boolean = false,
                      isInternal: Boolean = false,
                      arguments: Map[String, AnyRef] = Map.empty,
                      bindings: List[Binding] = List.empty)
      extends Declaration
      with StrictLogging {

    def notDurable: Exchange = copy(isDurable = false)

    def autoDelete: Exchange = copy(shouldAutoDelete = true)

    def internal: Exchange = copy(isInternal = true)

    def argument(value: (String, AnyRef)): Exchange = copy(arguments = this.arguments + value)

    def expires(value: FiniteDuration): Exchange = argument("x-expires" -> Long.box(value.toMillis))

    def binding(routingKeyToQueue: (RoutingKey, QueueName), arguments: Map[String, AnyRef] = Map.empty): Exchange =
      copy(bindings = this.bindings :+ Binding(name, routingKeyToQueue._2, routingKeyToQueue._1, arguments))

    override def run: (AmqpOps) => Try[Unit] = ops => {
      logger.info(
        s"Declaring Exchange($name, $exchangeType, isDurable=$isDurable, shouldAutoDelete=$shouldAutoDelete, isInternal=$isInternal, arguments=$arguments)")

      for {
        _ <- ops.declareExchange(this)
        _ <- TryUtil.sequence(bindings.map(_.run(ops)))
      } yield ()
    }
  }

  case class Binding(exchangeName: ExchangeName,
                     queueName: QueueName,
                     routingKey: RoutingKey,
                     arguments: Map[String, AnyRef])
      extends Declaration
      with StrictLogging {
    override def run: (AmqpOps) => Try[Unit] = ops => {
      logger.info(s"Declaring $this")
      ops.bindQueue(this)
    }
  }

  case class ExchangeBinding(destinationExchangeName: ExchangeName,
                             sourceExchangeName: ExchangeName,
                             routingKey: RoutingKey,
                             arguments: Map[String, AnyRef])
      extends Declaration
      with StrictLogging {
    override def run: (AmqpOps) => Try[Unit] = ops => {
      logger.info(s"Declaring $this")
      ops.bindExchange(this)
    }
  }

  case class Queue(name: QueueName,
                   isDurable: Boolean = true,
                   isExclusive: Boolean = false,
                   shouldAutoDelete: Boolean = false,
                   arguments: Map[String, AnyRef] = Map.empty)
      extends Declaration
      with StrictLogging {

    def notDurable: Queue = copy(isDurable = false)

    def exclusive: Queue = copy(isExclusive = true)

    def autoDelete: Queue = copy(shouldAutoDelete = true)

    def argument(value: (String, AnyRef)): Queue = copy(arguments = this.arguments + value)

    def expires(value: FiniteDuration): Queue = argument("x-expires" -> Long.box(value.toMillis))

    def deadLetterExchange(exchangeName: ExchangeName): Queue =
      argument("x-dead-letter-exchange" -> exchangeName.value)

    def messageTTL(value: FiniteDuration): Queue = argument("x-message-ttl" -> Long.box(value.toMillis))

    override def run: AmqpOps => Try[Unit] = ops => {
      logger.info(s"Declaring $this")
      ops.declareQueue(this)
    }
  }

}
