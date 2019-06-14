package com.itv.bucky.sqs

import cats.effect.Resource
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.itv.bucky
import com.itv.bucky.{AmqpClient, Handler, Publisher, consume, decl}

object SqsAmqpClient {

  //https://sqs.eu-west-1.amazonaws.com/829282787238/bucky-sqs-test-queue

  def apply[F[_]](sqsClient: AmazonSQS) = new AmqpClient[F] {
    override def declare(declarations: decl.Declaration*): F[Unit] = ???

    override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = ???

    override def publisher(): Publisher[F, consume.PublishCommand] = msg => sqsClient.sendMessage(msg.routingKey.value, msg.body.unmarshal[String].right.get)

    override def registerConsumer(queueName: bucky.QueueName, handler: Handler[F, consume.Delivery], exceptionalAction: consume.ConsumeAction, prefetchCount: Int): Resource[F, Unit] = ???

    override def isConnectionOpen: F[Boolean] = ???
  }

}
