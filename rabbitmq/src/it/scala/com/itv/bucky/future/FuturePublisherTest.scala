package com.itv.bucky.future

import com.itv.bucky._
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.itv.bucky.suite.{PublisherBaseTest, RequeueStrategy, TestFixture}
import com.itv.bucky.utils.FutureEffectVerification
import com.itv.lifecycle.Lifecycle
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

trait FuturePublisherTest extends PublisherBaseTest[Future] with ScalaFutures with FutureEffectVerification {
  import Implicits._

  override def withPublisher(testQueueName: QueueName,
                             requeueStrategy: RequeueStrategy[Future],
                             shouldDeclare: Boolean)(f: (TestFixture[Future]) => Unit): Unit = {
    val key          = RoutingKey(testQueueName.value)
    val exchangeName = ExchangeName("")
    val clientLifecycle = for {
      client <- AmqpClientLifecycle(utils.config)
      _      <- DeclarationLifecycle(utils.defaultDeclaration(testQueueName), client)
    } yield client

    Lifecycle.using(clientLifecycle) { amqpClient =>
      Lifecycle.using(amqpClient.publisher()) { publisher =>
        f(TestFixture[Future](publisher, key, exchangeName, testQueueName, amqpClient, None))

      }
    }
  }

}
