package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky._
import com.itv.bucky.fs2.utils._
import com.itv.bucky.suite.NoneRequeue

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually

import scala.language.higherKinds
import Eventually.eventually
import _root_.fs2.async
import cats.implicits._
import scala.concurrent.ExecutionContext.Implicits.global

class LoadPublishMessagesTest extends FunSuite with IOPublisherConsumerBaseTest with IOEffectVerification {

  test(s"Can publish messages in parallel to a (pre-existing) queue") {
    val handler = new StubConsumeHandler[IO, Delivery]

    withPublisherAndConsumer(requeueStrategy = NoneRequeue(handler)) { app =>
      val body = Any.payload()
      val size = 1000

      verifySuccess(
        async
          .parallelSequence((1 to size).toList
            .map(_ =>
              app.publisher(PublishCommand(app.exchangeName, app.routingKey, MessageProperties.textPlain, body))))
          .flatMap(_ => IO.unit))

      eventually {
        handler.receivedMessages should have size size
        handler.receivedMessages.head.body.value should ===(body.value)
      }
    }
  }
}
