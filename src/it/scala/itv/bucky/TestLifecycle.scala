package itv.bucky

import itv.bucky.decl.Declaration
import itv.contentdelivery.lifecycle.{Lifecycle, NoOpLifecycle}

import scala.concurrent.ExecutionContext

object TestLifecycle {
  import IntegrationUtils._

  val defaultConfig = AmqpClientConfig("localhost", 5672, "guest", "guest", networkRecoveryInterval = None)

  def base(declarations: List[Declaration], config: AmqpClientConfig = defaultConfig)
          (implicit executionContext: ExecutionContext): Lifecycle[(AmqpClient, Publisher[PublishCommand])] = {
    for {
      _ <- if (config.host == "localhost") LocalAmqpServer() else NoOpLifecycle(())
      amqpClient <- config
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
