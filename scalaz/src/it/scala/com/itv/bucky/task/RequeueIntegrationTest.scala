package com.itv.bucky.task

import com.itv.bucky.task.IntegrationUtils._
import com.itv.bucky.{Delivery, MessageProperties, PublishCommand, StubConsumeHandler}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scalaz.\/-
import scalaz.concurrent.Task

class RequeueIntegrationTest extends FunSuite with ScalaFutures with StrictLogging {

  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test(s"Can purge messages from a queue") {
    val handler = new StubConsumeHandler[Task, Delivery]()

    withPublisherAndConsumer(handler, requeueStrategy = SimpleRequeue) { app =>
      app.publish(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.persistentBasic, randomPayload())).unsafePerformSyncAttempt shouldBe \/-(())

    }
  }

}