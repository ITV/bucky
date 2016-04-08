package itv.bucky.examples

import itv.bucky._
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly
import itv.utils.Blob

import scala.concurrent.Future

case class PrintlnHandler(requeuePublisher: Publisher[Blob]) extends Handler[Blob] {

  override def apply(message: Blob): Future[ConsumeAction] = {
    val s = message.to[String]

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

  import BlobSerializer._

  implicit val messageSerializer: PublishCommandSerializer[Blob] =
    blobSerializer[Blob] using RoutingKey("bucky-basicconsumer-example.requeue") using Exchange("")

  val testQueueName = "bucky-basicconsumer-example"
  lazy val (testQueues, amqpClientConfig, rmqAdminHhttp) = IntegrationUtils.setUp(testQueueName, "bucky-basicconsumer-example.requeue")

  testQueues.foreach(_.purge())

  for {
    amqpClient <- amqpClientConfig
    requeue <- amqpClient.publisher()
    consumer <- amqpClient.consumer(testQueueName, PrintlnHandler(requeue))
  } {
    println("Started the consumer")
    Thread.sleep(1000000)
  }

}