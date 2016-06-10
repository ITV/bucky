package itv.bucky.examples

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import itv.bucky._
import itv.bucky.decl.DeclLifecycle
import itv.bucky.decl.pattern.Pattern
import itv.utils.Blob

import scala.concurrent.Future
import itv.contentdelivery.testutilities.SameThreadExecutionContext.implicitly

import scala.concurrent.duration._

case class Person(name: String)

object RequeueIfNotMartinHandler extends RequeueHandler[Person] with StrictLogging {

  override def apply(person: Person): Future[RequeueConsumeAction] =
    if (person.name == "Martin") {
      logger.info("Person was Martin, cool")
      Future.successful(Consume(Ack))
    }
    else {
      logger.info(s"$person is not Martin, requeueing")
      Future.successful(Requeue)
    }

}

object RequeueConsumer extends App {

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
      queueName = QueueName("requeue.consumer.example")
      _ <- DeclLifecycle(Pattern.Requeue(queueName, retryAfter = 10.seconds), client)
      _ <- AmqpClient.requeueHandlerOf(client)(queueName,
                                                RequeueIfNotMartinHandler,
                                                RequeuePolicy(maximumProcessAttempts = 3),
                                                deserializer)
    }
      yield ()

  consumerLifecycle.runUntilJvmShutdown()

}
