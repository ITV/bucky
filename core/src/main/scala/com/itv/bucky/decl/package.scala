package com.itv.bucky

import scala.concurrent.duration.FiniteDuration

package object decl {

  sealed trait Declaration
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
      extends Declaration {

    def notDurable: Exchange                        = copy(isDurable = false)
    def autoDelete: Exchange                        = copy(shouldAutoDelete = true)
    def internal: Exchange                          = copy(isInternal = true)
    def argument(value: (String, AnyRef)): Exchange = copy(arguments = this.arguments + value)
    def expires(value: FiniteDuration): Exchange    = argument("x-expires" -> Long.box(value.toMillis))
    def binding(routingKeyToQueue: (RoutingKey, QueueName), arguments: Map[String, AnyRef] = Map.empty): Exchange =
      copy(bindings = this.bindings :+ Binding(name, routingKeyToQueue._2, routingKeyToQueue._1, arguments))
  }

  case class Binding(exchangeName: ExchangeName, queueName: QueueName, routingKey: RoutingKey, arguments: Map[String, AnyRef]) extends Declaration
  case class ExchangeBinding(destinationExchangeName: ExchangeName,
                             sourceExchangeName: ExchangeName,
                             routingKey: RoutingKey,
                             arguments: Map[String, AnyRef] = Map.empty)
      extends Declaration
  case class Queue(name: QueueName,
                   isDurable: Boolean = true,
                   isExclusive: Boolean = false,
                   shouldAutoDelete: Boolean = false,
                   arguments: Map[String, AnyRef] = Map.empty)
      extends Declaration {
    def notDurable: Queue                                     = copy(isDurable = false)
    def exclusive: Queue                                      = copy(isExclusive = true)
    def autoDelete: Queue                                     = copy(shouldAutoDelete = true)
    def argument(value: (String, AnyRef)): Queue              = copy(arguments = this.arguments + value)
    def expires(value: FiniteDuration): Queue                 = argument("x-expires" -> Long.box(value.toMillis))
    def deadLetterExchange(exchangeName: ExchangeName): Queue = argument("x-dead-letter-exchange" -> exchangeName.value)
    def messageTTL(value: FiniteDuration): Queue              = argument("x-message-ttl" -> Long.box(value.toMillis))
    def maxPriority(value: Option[Int]): Queue                = value.fold(this)(max => argument("x-max-priority" -> Long.box(max)))
  }
}
