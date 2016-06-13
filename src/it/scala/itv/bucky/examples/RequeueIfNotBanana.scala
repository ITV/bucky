package itv.bucky.examples

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import itv.bucky._
import itv.bucky.pattern.requeue._
import itv.bucky.decl.DeclarationLifecycle
import itv.contentdelivery.lifecycle.Lifecycle
import itv.utils.Blob

import scala.concurrent.Future
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

import scala.concurrent.duration._

case class Fruit(name: String)

object Fruit {
  val deserializer: BlobDeserializer[Fruit] = new BlobDeserializer[Fruit] {
    override def apply(blob: Blob): DeserializerResult[Fruit] = {
      import DeserializerResult._
      Fruit(blob.to[String]).success
    }
  }
}

object RequeueIfNotBananaHandler extends RequeueHandler[Fruit] with StrictLogging {

  override def apply(fruit: Fruit): Future[RequeueConsumeAction] =
    fruit match {
      case Fruit("Banana") => {
        logger.info("Fruit was Banana, cool")
        Future.successful(Ack)
      }
      case _ => {
        logger.info(s"$fruit is not Banana, requeueing")
        Future.successful(Requeue)
      }
    }

}

case class RequeueIfNotBanana(clientLifecycle: Lifecycle[AmqpClient]) {

  val queueName = QueueName("requeue.consumer.example")

  val consumerLifecycle =
    for {
      client <- clientLifecycle
      _ <- DeclarationLifecycle(requeueDeclarations(queueName, retryAfter = 10.seconds), client)
      _ <- client.requeueHandlerOf(queueName,
        RequeueIfNotBananaHandler,
        RequeuePolicy(maximumProcessAttempts = 3),
        Fruit.deserializer)
    }
      yield ()

}

object Main extends App {
  def readConfig(config: Config): AmqpClientConfig = {
    val host = config.getString("rmq.host")
    val clientConfig = config.getConfig("rmq.client")
    AmqpClientConfig(host, clientConfig.getInt("port"), clientConfig.getString("username"), clientConfig.getString("password"))
  }

  RequeueIfNotBanana(readConfig(ConfigFactory.load("bucky"))).consumerLifecycle.runUntilJvmShutdown()
}
