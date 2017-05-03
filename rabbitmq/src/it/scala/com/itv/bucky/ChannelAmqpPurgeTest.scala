package com.itv.bucky

import com.itv.bucky.lifecycle._
import com.itv.lifecycle.Lifecycle
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.{Random, Success}

class ChannelAmqpPurgeTest extends FunSuite with ScalaFutures with StrictLogging {

  implicit val eventuallyPatienceConfig = Eventually.PatienceConfig(5.seconds, 1.second)

  test(s"Can purge messages from a queue") {

    val randomQueueName = QueueName("purgeQueueTest" + Random.nextInt())

    val clientLifecycle = for {
      client <- AmqpClientLifecycle(IntegrationUtils.config)
      _ <- DeclarationLifecycle(IntegrationUtils.defaultDeclaration(randomQueueName), client)
      publisher <- client.publisher()
      _ = publisher(PublishCommand(ExchangeName(""), RoutingKey(randomQueueName.value), MessageProperties.persistentBasic, Payload.from("")))
    }
      yield client

    Lifecycle.using(clientLifecycle) { client =>
      Eventually.eventually {
        client.estimatedMessageCount(randomQueueName) shouldBe Success(1)
      }
      client.performOps(amqpOps => amqpOps.purgeQueue(randomQueueName))
      Eventually.eventually {
        client.estimatedMessageCount(randomQueueName) shouldBe Success(0)
      }
    }
  }

}