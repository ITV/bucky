package itv.bucky.examples

import itv.bucky.BlobSerializer.{BlobSerializer => _}
import itv.bucky._
import itv.contentdelivery._
import itv.contentdelivery.lifecycle.Lifecycle
import itv.contentdelivery.metrics.MetricsRegistries
import itv.contentdelivery.scalatra.ServletBootstrap
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.{Blob, BlobMarshaller}

import scala.concurrent.Future

case class MyMessage(foo: String)

case class PrintlnHandler(requeuePublisher: Publisher[MyMessage]) extends Handler[MyMessage] {

  override def apply(message: MyMessage): Future[ConsumeAction] = {
    val s = message.foo

    if (s contains "Foo") {
      println(s)
      Future.successful(Ack)
    }
    else {
      println("Requeue")
      requeuePublisher(message).map(_ => Ack)
    }
  }

}

object BasicConsumer extends App {
  new BasicConsumerLifecycle().apply(Config("bucky-basicconsumer-example")).runUntilJvmShutdown()

  import BlobSerializer._
  import DeserializerResult._


  case class Config(queueName: String) extends MicroServiceConfig {
    override def webServer: WebServer = WebServer(8080)

    override def metaInfo: MetaInfo = MetaInfo(Active, None)
  }
  class BasicConsumerLifecycle extends MicroService[Config] {
    override protected def mainService(config: Config, registries: MetricsRegistries): Lifecycle[ServletBootstrap] = {
        val queueName = config.queueName

        implicit val messageDeserializer = new BlobDeserializer[MyMessage] {
          override def apply(blob: Blob): DeserializerResult[MyMessage] = MyMessage(blob.to[String]).success
        }

      implicit val messageMarshaller: BlobMarshaller[MyMessage] = BlobMarshaller[MyMessage] {
        message => Blob.from(message.foo)
      }

      val myMessageSerializer = blobSerializer[MyMessage] using RoutingKey(queueName + ".requeue") using Exchange("")


      lazy val (testQueues, amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(queueName, queueName + ".requeue")

        testQueues.foreach(_.purge())

        import AmqpClient._
        for {
          amqClient <- buildAmqpClient(amqpClientConfig)
          publisher <- amqClient.publisher().map(publisherOf[MyMessage](myMessageSerializer))
          blah <- amqClient.consumer(queueName, handlerOf[MyMessage](PrintlnHandler(publisher)))
        } yield {
          println("Started the consumer")
          ServletBootstrap.default

        }
    }

    protected def buildAmqpClient(amqpClientConfig: AmqpClientConfig): Lifecycle[AmqpClient] = amqpClientConfig
  }


}