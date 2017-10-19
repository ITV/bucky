package com.itv.bucky.template

import com.itv.bucky._
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.language.higherKinds

trait PublisherIntegrationTest[F[_], E]
    extends FunSuite
    with PublisherConsumerBaseTest[F]
    with StrictLogging
    with EffectMonad[F, Throwable]
    with Eventually {

  implicit val consumerPatienceConfig: Eventually.PatienceConfig = Eventually.PatienceConfig(timeout = 90.seconds)

  test(s"Can publish messages to a (pre-existing) queue") {
    val handler = new StubConsumeHandler[F, Delivery]

    withPublisherAndConsumer(requeueStrategy = NoneRequeue(handler)) { app =>
      val body = Any.payload()

      verifySuccess(app.publisher(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.textPlain, body)))

      eventually {
        handler.receivedMessages should have size 1
        handler.receivedMessages.head.body.value should ===(body.value)
      }
    }
  }
}
