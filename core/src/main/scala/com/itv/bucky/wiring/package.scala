package com.itv.bucky

import scala.reflect.runtime.universe._

package object wiring {

//  private def getFields[T: TypeTag]: List[String] =
//    typeOf[T].members.sorted.collect {
//      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
//    }

  object implicits {
    implicit class Docs[T](w: Wiring[T]) {
      def document: String = s"ExchangeName: ${w.exchangeName}\nRoutingKey: ${w.routingKey}\nQueueName: ${w.queueName}}\nPayload: TODO"
    }
  }
}
