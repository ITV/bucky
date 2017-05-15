package com.itv.bucky

import com.itv.bucky.decl.Declaration
import com.itv.bucky.lifecycle._
import com.itv.lifecycle.Lifecycle

import scala.concurrent.{ExecutionContext, Future}

object TestLifecycle {
  import IntegrationUtils._

  val defaultConfig = AmqpClientConfig("localhost", 5672, "guest", "guest", networkRecoveryInterval = None)

  def base(declarations: List[Declaration], config: AmqpClientConfig = defaultConfig)
          (implicit executionContext: ExecutionContext): Lifecycle[(AmqpClient[Lifecycle, Future, Throwable, Unit], Publisher[Future, PublishCommand])] = {
    for {
      amqpClient <- AmqpClientLifecycle(config)
      _ <- DeclarationLifecycle(declarations, amqpClient)
      publisher <- amqpClient.publisher()
    } yield (amqpClient, publisher)
  }

  def rawConsumerWithDeclaration[T](queueName: QueueName, handler: Handler[Future, Delivery], declarations: List[Declaration], config: AmqpClientConfig = defaultConfig)
                                   (implicit executionContext: ExecutionContext) = for {
    result <- base(declarations, config)
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, handler)
  } yield publisher

  def rawConsumer[T](queueName: QueueName, handler: Handler[Future, Delivery], config: AmqpClientConfig = defaultConfig)
                    (implicit executionContext: ExecutionContext) = for {
    result <- base(defaultDeclaration(queueName), config)
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, handler)
  } yield publisher

}
