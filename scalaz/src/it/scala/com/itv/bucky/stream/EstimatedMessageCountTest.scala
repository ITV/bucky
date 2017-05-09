package com.itv.bucky.stream

import com.itv.bucky.stream.IntegrationUtils._
import com.itv.bucky.{MessageProperties, PublishCommand}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.util.{Random, Success}
import scalaz.concurrent.Task

class EstimatedMessageCountTest extends FunSuite {

  test("A new queue with no messages published should have an estimated count of 0") {
    estimatedMessageCountTest(0)
  }

  test("A new queue with 1 message published should have an estimated count of 1") {
    estimatedMessageCountTest(1)
  }

  test("A new queue with n messages published should have an estimated count of n") {
    estimatedMessageCountTest(Random.nextInt(10))
  }

  implicit val patianceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 5.seconds, 1.second)

  def estimatedMessageCountTest(messagesToPublish: Int): Unit = {
    val queueName = randomQueue()

    withPublisher(queueName) { app =>
      Task.gatherUnordered(
        (1 to messagesToPublish).map(_ =>
          app.publish(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, randomPayload()))
        )
      ).unsafePerformSyncAttempt
    }
    withPublisher(queueName, shouldDeclare = false) {app =>
      Eventually.eventually {
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(messagesToPublish)
      }
    }

  }

}
