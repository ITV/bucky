package com.itv.bucky.sqs

import cats.effect.{ConcurrentEffect, ExitCode, IO, IOApp, Resource}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.itv.bucky
import com.itv.bucky.{AmqpClient, ExchangeName, Handler, MessagePropertiesConverters, Payload, Publisher, QueueName, RoutingKey, consume, decl}
import cats.implicits._
import com.itv.bucky.consume.{Ack, ConsumeAction, PublishCommand}
import com.itv.bucky.publish.MessageProperties
import com.rabbitmq.client.Delivery

import scala.language.higherKinds

object SqsAmqpClient {

  //https://sqs.eu-west-1.amazonaws.com/829282787238/bucky-sqs-test-queue

  def apply[F[_]](clientBuilder: AmazonSQSClientBuilder)(implicit F: ConcurrentEffect[F]): Resource[F, AmqpClient[F]] = {
    val client: Resource[F, AmazonSQS] = Resource.make(F.delay(clientBuilder.build()))(c => F.delay(c.shutdown()))
    client.map { sqs =>
      new AmqpClient[F] {
        override def declare(declarations: decl.Declaration*): F[Unit] = ???

        override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = ???

        override def publisher(): Publisher[F, consume.PublishCommand] = msg => F.delay {
          sqs.sendMessage(msg.routingKey.value, msg.body.unmarshal[String].right.get)
          ()
        }

        override def registerConsumer(queueName: bucky.QueueName, handler: Handler[F, consume.Delivery], exceptionalAction: consume.ConsumeAction, prefetchCount: Int): Resource[F, Unit] = ???

        override def isConnectionOpen: F[Boolean] = ???
      }
    }
  }

}


object Main extends IOApp {

  val handler: Handler[IO, consume.Delivery] = (v1: consume.Delivery) => IO {
    println(v1.body.unmarshal[String].right.get)
    Ack
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val builder =
      AmazonSQSClientBuilder.standard()
    builder.setRegion("eu-west-1")

    val queueName = "bucky-sqs-test-queue"

    val clientResource =
      for {
        client <- SqsAmqpClient[IO](builder)
        _ <- client.registerConsumer(QueueName(queueName), handler)
      }
        yield client

    clientResource.use { amqpClient =>
      val publisher = amqpClient.publisher()
      publisher(PublishCommand(ExchangeName(""), RoutingKey(queueName), MessageProperties.basic, Payload.from[String]("hello, world!")))
    } *> IO.pure(ExitCode.Success)
  }

}