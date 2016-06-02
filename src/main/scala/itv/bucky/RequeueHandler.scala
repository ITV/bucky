package itv.bucky

import com.rabbitmq.client.AMQP

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try
import scala.collection.JavaConverters._

case class RequeueHandler[T](requeuePublisher: Publisher[PublishCommand], requeueExchange: Exchange, requeuePolicy: RequeuePolicy)(handler: Handler[Delivery])
                            (implicit executionContext: ExecutionContext) extends Handler[Delivery] {

  implicit class BasicPropertiesOps(val basicProperties: AMQP.BasicProperties) {

    def headersAsMap: Map[String, AnyRef] =
      Option(basicProperties.getHeaders).fold(Map.empty[String, AnyRef])(_.asScala.toMap)

  }

  private val requeueCountHeaderName = "x-bucky-requeue-counter"

  private def remainingAttempts(delivery: Delivery): Option[Int] =
    for {
      requeueCountAnyRef <- delivery.properties.headersAsMap.get(requeueCountHeaderName)
      requeueCount <- Try(requeueCountAnyRef.toString.toInt).toOption
    }
      yield requeueCount

  private def buildRequeuePublishCommand(delivery: Delivery, remainingAttempts: Int): PublishCommand = {
    val newHeader: (String, AnyRef) = requeueCountHeaderName -> remainingAttempts.toString
    val headers = (delivery.properties.headersAsMap + newHeader).asJava

    val properties = delivery.properties.builder().headers(headers).build()
    PublishCommand(requeueExchange, RoutingKey(delivery.envelope.getRoutingKey), properties, delivery.body)
  }

  override def apply(delivery: Delivery): Future[ConsumeAction] =
    handler(delivery) flatMap {

      case Requeue => {
        remainingAttempts(delivery) match {
          case Some(value) if value < 1 => Future.successful(DeadLetter)
          case Some(value) => requeuePublisher(buildRequeuePublishCommand(delivery, value - 1)).map(_ => Requeue)
          case None => {
            if (requeuePolicy.maximumProcessAttempts <= 1)
              Future.successful(DeadLetter)
            else {
              val initialRemainingAttempts = requeuePolicy.maximumProcessAttempts - 2
              requeuePublisher(buildRequeuePublishCommand(delivery, initialRemainingAttempts)).map(_ => Requeue)
            }
          }
        }
      }

      case result => Future.successful(result)

    }
}
