package com.itv.bucky.suite

import com.itv.bucky._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually

import scala.language.higherKinds
import scala.concurrent.duration._
import scala.language.postfixOps

trait LoadPublishMessagesTest[F[_]]
    extends FunSuite
    with PublisherConsumerBaseTest[F]
    with EffectMonad[F, Throwable]
    with ParallelEffectMonad[F] {

  def numberRequestInParallel = 100

  implicit val patienceConfig: Eventually.PatienceConfig =
    Eventually.PatienceConfig(timeout = 10 seconds, interval = 100 millis)

  test(s"Can publish messages in parallel to a (pre-existing) queue") {
    val handler = new StubConsumeHandler[F, Delivery]

    withPublisherAndConsumer(requeueStrategy = NoneRequeue(handler)) { app =>
      val body = Any.payload()

      verifySuccess(
        effectMonad.flatMap(sequence((1 to numberRequestInParallel).toList
          .map(_ =>
            app.publisher(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.textPlain, body)))))(_ =>
          effectMonad.apply[Unit](())))

      eventually {
        handler.receivedMessages should have size numberRequestInParallel
        handler.receivedMessages.head.body.value should ===(body.value)
      }
    }
  }
}
