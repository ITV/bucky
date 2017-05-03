package com.itv.bucky

import java.io.File

import com.itv.bucky.decl.{DeclarationLifecycle, _}
import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}

import scala.concurrent.ExecutionContext

object TestLifecycle {
  import IntegrationUtils._

  val defaultConfig = AmqpClientConfig("localhost", 5672, "guest", "guest", networkRecoveryInterval = None)

  def base(declarations: List[Declaration], config: AmqpClientConfig = defaultConfig)
          (implicit executionContext: ExecutionContext): Lifecycle[(AmqpClient[Lifecycle], Publisher[PublishCommand])] = {
    for {
      _ <- if (config.host == "localhost") LocalAmqpServer(passwordFile = new File("src/it/resources/qpid-passwd")) else NoOpLifecycle(())
      amqpClient <- AmqpClientLifecycle(config)
      _ <- DeclarationLifecycle(declarations, amqpClient)
      publisher <- amqpClient.publisher()
    } yield (amqpClient, publisher)
  }

  def rawConsumerWithDeclaration[T](queueName: QueueName, handler: Handler[Delivery], declarations: List[Declaration], config: AmqpClientConfig = defaultConfig)
                                   (implicit executionContext: ExecutionContext) = for {
    result <- base(declarations, config)
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, handler)
  } yield publisher

  def rawConsumer[T](queueName: QueueName, handler: Handler[Delivery], config: AmqpClientConfig = defaultConfig)
                    (implicit executionContext: ExecutionContext) = for {
    result <- base(defaultDeclaration(queueName), config)
    (amqClient, publisher) = result
    consumer <- amqClient.consumer(queueName, handler)

  } yield publisher

}
