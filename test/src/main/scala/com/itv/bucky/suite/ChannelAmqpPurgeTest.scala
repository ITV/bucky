package com.itv.bucky.suite

import com.itv.bucky.{Any, MessageProperties, PublishCommand}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.Success

trait ChannelAmqpPurgeTest[F[_]] extends FunSuite with PublisherBaseTest[F] with StrictLogging {
  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test(s"Can purge messages from a queue") {
    val queueName = Any.queue()

    withPublisher(queueName) { app =>
      logger.info(s"Publish message on $queueName")
      verifySuccess(
        app.publisher(
          PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, Any.payload())))
    }
    withPublisher(queueName, shouldDeclare = false) { app =>
      Eventually.eventually {
        logger.info(s"Estimate message on $queueName")
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(1)
      }
      logger.info(s"Purge $queueName")
      app.amqpClient.performOps(amqpOps => amqpOps.purgeQueue(queueName))
      Eventually.eventually {
        logger.info(s"Estimate message on $queueName")
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(0)
      }
    }
  }

}
