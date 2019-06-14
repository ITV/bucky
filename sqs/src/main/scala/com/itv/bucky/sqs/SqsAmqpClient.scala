package com.itv.bucky.sqs

import cats.arrow.FunctionK
import cats.effect._
import cats.implicits._
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSAsyncClient, AmazonSQSAsyncClientBuilder, AmazonSQSClientBuilder}
import com.itv.bucky
import com.itv.bucky.consume.{Ack, ConsumeAction, ConsumerTag, Delivery, PublishCommand}
import com.itv.bucky.publish.MessageProperties
import com.itv.bucky.{AmqpClient, Envelope, ExchangeName, Handler, Payload, Publisher, QueueName, RoutingKey, consume, decl}

import scala.collection.JavaConverters._
import scala.language.higherKinds

object SqsAmqpClient {

  //https://sqs.eu-west-1.amazonaws.com/829282787238/bucky-sqs-test-queue

  def apply[F[_]](clientBuilder: AmazonSQSAsyncClientBuilder)(implicit F: ConcurrentEffect[F], contextShift: ContextShift[IO]): Resource[F, AmqpClient[F]] = {
    val client: Resource[F, AmazonSQSBufferedAsyncClient] = Resource.make(F.delay(clientBuilder.build()))(c => F.delay(c.shutdown()))
      .map(new AmazonSQSBufferedAsyncClient(_))
    client.map { sqs =>
      new AmqpClient[F] {
        override def declare(declarations: decl.Declaration*): F[Unit] = ???

        override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = ???

        override def publisher(): Publisher[F, consume.PublishCommand] = msg => F.delay {
          sqs.sendMessage(msg.routingKey.value, msg.body.unmarshal[String].right.get)
          ()
        }

        override def registerConsumer(queueName: bucky.QueueName, handler: Handler[F, consume.Delivery], exceptionalAction: consume.ConsumeAction, prefetchCount: Int): Resource[F, Unit] = {
          Resource.make(F.toIO(F.delay(awaitMessages(queueName, handler))).start)(_.cancel).mapK[F](FunctionK.lift[IO, F](F.liftIO)).map(_ => ())
        }

        override def isConnectionOpen: F[Boolean] = ???

        private def awaitMessages(queueName: QueueName, handler: Handler[F, consume.Delivery]) = {
          while (true) {
            val messageRequest = new ReceiveMessageRequest()
              .withMaxNumberOfMessages(1)
              .withQueueUrl(queueName.value)
            val result = sqs.receiveMessage(messageRequest)
            result.getMessages.asScala.headOption.foreach{ msg =>
              val delivery = Delivery(Payload.from[String](msg.getBody), ConsumerTag("foo"), Envelope(0L, redeliver = false, ExchangeName(""), RoutingKey(queueName.value)), MessageProperties.basic)
              F.toIO[ConsumeAction](handler.apply(delivery)).unsafeRunSync()
            }
          }
        }
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
    val builder = AmazonSQSAsyncClientBuilder.standard()
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
    } *> IO.never.as(ExitCode.Success)
  }

}