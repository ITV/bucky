package itv.bucky.examples

import com.typesafe.scalalogging.StrictLogging
import itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import itv.bucky._
import itv.contentdelivery._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.metrics.MetricsRegistries
import itv.contentdelivery.scalatra.ServletBootstrap
import itv.bucky.SameThreadExecutionContext.implicitly
import itv.bucky.Unmarshaller.StringPayloadUnmarshaller

import scala.concurrent.Future

case class MyMessage(foo: String)

case class PrintlnHandler(targetPublisher: Publisher[MyMessage]) extends Handler[MyMessage] with StrictLogging {

  override def apply(message: MyMessage): Future[ConsumeAction] = {
    val s = message.foo

    if (s contains "Foo") {
      logger.info(s)
      Future.successful(Ack)
    }
    else {
      logger.info(s"Target $message")
      targetPublisher(message).map(_ => Ack)
    }
  }

}

object BasicConsumer extends App {
  new BasicConsumerLifecycle().apply(Config(QueueName("bucky-basicconsumer-example"), QueueName("target-bucky-basicconsumer-example"))).runUntilJvmShutdown()

  import PublishCommandBuilder._


  case class Config(queueName: QueueName, targetQueueName: QueueName) extends MicroServiceConfig {
    override def webServer: WebServer = WebServer(8080)

    override def metaInfo: MetaInfo = MetaInfo(Active, None)
  }

  class BasicConsumerLifecycle extends MicroService[Config] with StrictLogging {
    override protected def mainService(config: Config, registries: MetricsRegistries): Lifecycle[ServletBootstrap] = {
        import config._

      val payloadUnmarshaller = StringPayloadUnmarshaller.map(MyMessage)

      val messageMarshaller: PayloadMarshaller[MyMessage] = StringPayloadMarshaller.contramap(_.foo)

      val myMessageSerializer = publishCommandBuilder(messageMarshaller) using RoutingKey(targetQueueName.value) using ExchangeName("")

      val testQueues = IntegrationUtils.declareQueues(QueueName(queueName.value), QueueName(targetQueueName.value))

        import AmqpClient._
        for {
          amqClient <- buildAmqpClient(IntegrationUtils.config)
          publisher <- amqClient.publisher().map(publisherOf(myMessageSerializer))
          blah <- amqClient.consumer(queueName, handlerOf(PrintlnHandler(publisher), payloadUnmarshaller))
        } yield {
          logger.info("Started the consumer")
          ServletBootstrap.default

        }
    }

    protected def buildAmqpClient(amqpClientConfig: AmqpClientConfig): Lifecycle[AmqpClient] = amqpClientConfig
  }


}