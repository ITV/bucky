package itv.bucky.pattern.requeue

import com.rabbitmq.client.AMQP
import com.typesafe.scalalogging.StrictLogging
import itv.bucky.{Ack, ConsumeAction, DeadLetter, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.collection.JavaConverters._

case class RequeueTransformer(requeuePublisher: Publisher[PublishCommand],
                              requeueExchange: ExchangeName,
                              requeuePolicy: RequeuePolicy,
                              onFailure: RequeueConsumeAction)(handler: RequeueHandler[Delivery])
                             (implicit executionContext: ExecutionContext) extends Handler[Delivery] with StrictLogging {

  private val requeueCountHeaderName = "x-bucky-requeue-counter"

  implicit class BasicPropertiesOps(val basicProperties: AMQP.BasicProperties) {
    def headersAsMap: Map[String, AnyRef] =
      Option(basicProperties.getHeaders).fold(Map.empty[String, AnyRef])(_.asScala.toMap)
  }

  private def remainingAttempts(delivery: Delivery): Option[Int] = for {
    requeueCountAnyRef <- delivery.properties.headersAsMap.get(requeueCountHeaderName)
    requeueCount <- Try(requeueCountAnyRef.toString.toInt).toOption
  } yield requeueCount

  private def buildRequeuePublishCommand(delivery: Delivery, remainingAttempts: Int): PublishCommand = {
    val newHeader: (String, AnyRef) = requeueCountHeaderName -> remainingAttempts.toString
    val headers = (delivery.properties.headersAsMap + newHeader).asJava

    val properties = delivery.properties.builder().headers(headers).build()
    PublishCommand(requeueExchange, RoutingKey(delivery.envelope.getRoutingKey), properties, delivery.body)
  }

  override def apply(delivery: Delivery): Future[ConsumeAction] = {
    def perform(action: RequeueConsumeAction) =
      action match {
        case itv.bucky.Requeue => remainingAttempts(delivery) match {
          case Some(value) if value < 1 => Future.successful(DeadLetter)
          case Some(value) => requeuePublisher(buildRequeuePublishCommand(delivery, value - 1)).map(_ => Ack)
          case None =>
            if (requeuePolicy.maximumProcessAttempts <= 1)
              Future.successful(DeadLetter)
            else {
              val initialRemainingAttempts = requeuePolicy.maximumProcessAttempts - 2
              requeuePublisher(buildRequeuePublishCommand(delivery, initialRemainingAttempts)).map(_ => Ack)
            }
        }
        case other: ConsumeAction => Future.successful(other)
      }

    safePerform(handler(delivery)) flatMap perform recoverWith {
      case t: Throwable => {
        logger.error(s"Unable to process ${delivery.body} due to handler failure, will $onFailure", t)
        perform(onFailure)
      }
    }
  }



}