package com.itv.bucky.task

import com.itv.bucky._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scalaz.\/-
import scalaz.concurrent.Task
import IntegrationUtils._


class PublisherIntegrationTest extends FunSuite with ScalaFutures with StrictLogging with Eventually {
  implicit val consumerPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)


  test(s"Can publish messages to a (pre-existing) queue") {

    val handler = new StubConsumeHandler[Task, Delivery]

    withPublisherAndConsumer(requeueStrategy = NoneRequeue(handler)) { app =>
      val body = randomPayload()

      app.publisher(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.textPlain, body)).unsafePerformSyncAttempt shouldBe \/-(())

      eventually {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head.body.value should ===(body.value)
      }

    }
  }


}
