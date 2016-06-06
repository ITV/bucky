package itv.bucky.examples

import com.typesafe.config.{Config, ConfigFactory}
import itv.bucky._
import itv.utils.Blob
import scala.concurrent.Future
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

case class Person(name: String)

object RequeueIfNotMartinHandler extends RequeueHandler[Person] {

  override def apply(person: Person): Future[RequeueConsumeAction] =
    if (person.name == "Martin") {
      println("Person was Martin, cool")
      Future.successful(Consume(Ack))
    }
    else {
      println(s"$person is not Martin, requeueing")
      Future.successful(Requeue)
    }

}

class RequeueConsumer extends App {

  def readConfig(config: Config): AmqpClientConfig = {
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
  }

  val config = readConfig(ConfigFactory.load("bucky"))

  val deserializer: BlobDeserializer[Person] = new BlobDeserializer[Person] {
    override def apply(blob: Blob): DeserializerResult[Person] = {
      import DeserializerResult._
      Person(blob.to[String]).success
    }
  }

  val consumerLifecycle =
    for {
      client <- config
      _ <- AmqpClient.requeueHandlerOf(client)(QueueName("requeue.consumer"),
                                                RequeueIfNotMartinHandler,
                                                RequeuePolicy(maximumProcessAttempts = 3),
                                                deserializer)
    }
      yield ()

  consumerLifecycle.runUntilJvmShutdown()

}
