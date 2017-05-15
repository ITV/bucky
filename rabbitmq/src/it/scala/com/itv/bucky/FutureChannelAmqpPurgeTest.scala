package com.itv.bucky

import com.itv.bucky.SameThreadExecutionContext.implicitly
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.Lifecycle
import org.scalatest.Assertion

import scala.concurrent.Future

import org.scalatest.Matchers._

import BuckyUtils._

class FutureChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[Future] {
  override def withPublisher(testQueueName: QueueName, requeueStrategy: RequeueStrategy[Future], shouldDeclare: Boolean)
                            (f: (TestFixture[Future]) => Unit): Unit = {
    val key = RoutingKey(testQueueName.value)
    val exchangeName = ExchangeName("")
    val clientLifecycle = for {
      client <- AmqpClientLifecycle(IntegrationUtils.config)
      _ <- DeclarationLifecycle(IntegrationUtils.defaultDeclaration(testQueueName), client)
    }  yield client

    Lifecycle.using(clientLifecycle) { amqpClient =>
      Lifecycle.using(amqpClient.publisher()) { publisher =>
        f(TestFixture[Future](publisher, key, exchangeName, testQueueName, amqpClient, None))

      }
    }
  }

  override def verifySuccess(f: Future[Unit]): Assertion = f.asTry.futureValue shouldBe 'success
}