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

  test(s"A new queue with n messages published should have an estimated count of n") {
    estimatedMessageCountTest(Random.nextInt(10))
  }

  test(s"A new queue with n messages published should have an estimated count of n composing tasks") {
    //In this case, RabbitMQ selects the same delivery tag for multiple messages
    estimatedMessageCountTest(Random.nextInt(10), composingTask = true)
  }

  implicit val patianceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 5.seconds, 1.second)

  def estimatedMessageCountTest(messagesToPublish: Int, composingTask: Boolean = false): Unit = {
    val queueName = randomQueue()

    withPublisher(queueName) { app =>
        val tasks = (1 to messagesToPublish).map(_ =>
          app.publish(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, randomPayload()))
        )
      if (composingTask)
        Task.gatherUnordered(tasks).unsafePerformSync
        else
      tasks.foreach(_.unsafePerformSyncAttempt)
    }
    withPublisher(queueName, shouldDeclare = false) { app =>
      Eventually.eventually {
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(messagesToPublish)
      }
    }

  }

}
