package com.itv.bucky.template

import com.itv.bucky.{Any, MessageProperties, PublishCommand}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.{Random, Success}

trait EstimatedMessageCountTest[F[_]] extends FunSuite with PublisherBaseTest[F] with StrictLogging {

  def runAll(list: Seq[F[Unit]]): Unit

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

  def estimatedMessageCountTest(messagesToPublish: Int, composingTask: Boolean = false): Unit = {
    val queueName = Any.queue()

    withPublisher(queueName) { app =>
      val tasks: Seq[F[Unit]] = (1 to messagesToPublish).map { i =>
        logger.info(s"Publish message: $i")
        app.publisher(
          PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, Any.payload()))
      }
      runAll(tasks)
    }
    withPublisher(queueName, shouldDeclare = false) { app =>
      Eventually.eventually {
        logger.info(s"Estimate message count on $queueName")
        app.amqpClient.estimatedMessageCount(queueName) shouldBe Success(messagesToPublish)
      }
    }

  }

}
