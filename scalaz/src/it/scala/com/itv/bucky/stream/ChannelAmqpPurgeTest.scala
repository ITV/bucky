package com.itv.bucky.stream

import com.itv.bucky.stream.IntegrationUtils._
import com.itv.bucky.{MessageProperties, PublishCommand}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Success
import scalaz.\/-

class ChannelAmqpPurgeTest extends FunSuite with ScalaFutures with StrictLogging {

  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test(s"Can purge messages from a queue") {
    val queueName = randomQueue()

    withPublisher(queueName) { app =>
      app.publish(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, randomPayload())).unsafePerformSyncAttempt shouldBe \/-(())

    }
    withPublisher(queueName, shouldDeclare = false) { app =>
      Eventually.eventually {
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(1)
      }
      app.amqpClient.performOps(amqpOps => amqpOps.purgeQueue(queueName))
      Eventually.eventually {
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(0)
      }
    }

  }

}