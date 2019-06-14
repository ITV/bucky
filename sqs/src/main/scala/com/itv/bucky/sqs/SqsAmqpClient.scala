package com.itv.bucky.sqs

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSAsyncClient, AmazonSQSAsyncClientBuilder, AmazonSQSClientBuilder}
import com.itv.bucky
import com.itv.bucky.consume.{Ack, ConsumeAction, ConsumerTag, DeadLetter, Delivery, PublishCommand}
import com.itv.bucky.publish.MessageProperties
import com.itv.bucky.{AmqpClient, Envelope, ExchangeName, Handler, Payload, Publisher, QueueName, RoutingKey, consume, decl}
import com.itv.bucky.decl._

import scala.collection.JavaConverters._
import scala.language.higherKinds

object SqsAmqpClient {

  //https://sqs.eu-west-1.amazonaws.com/829282787238/bucky-sqs-test-queue


  def apply[F[_]](clientBuilder: AmazonSQSAsyncClientBuilder)(implicit F: ConcurrentEffect[F], contextShift: ContextShift[IO]): Resource[F, AmqpClient[F]] = {

    val resources = F.toIO(Ref.of[F, List[(ExchangeName, QueueName, RoutingKey)]](List.empty)).unsafeRunSync()
    val deadLetterMappings = F.toIO(Ref.of[F, Map[QueueName, ExchangeName]](Map.empty)).unsafeRunSync()

    val client: Resource[F, AmazonSQSBufferedAsyncClient] = Resource.make(F.delay(clientBuilder.build()))(c => F.delay(c.shutdown()))
      .map(new AmazonSQSBufferedAsyncClient(_))
    client.map { sqs =>
      new AmqpClient[F] {
        override def declare(declarations: decl.Declaration*): F[Unit] = {
          declare(declarations.toIterable)
        }

        override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = {
          declarations.toList.traverse[F, Unit] {
            case Binding(ex, q, rk, _) =>
              resources.update(_ :+ (ex, q, rk))
            case Exchange(_, _, _, _, _, _, bindings) =>
              bindings.traverse[F, Unit] { binding =>
                resources.update(_ :+ (binding.exchangeName, binding.queueName, binding.routingKey))
              }.void
            case Queue(name, _, _, _, arguments) => for {
              _  <- F.delay(sqs.createQueue(name.value)).void
              _ <- arguments.get("x-dead-letter-exchange").fold(F.unit){exchange =>
                deadLetterMappings.update(_ + (name -> ExchangeName(exchange.toString)))}
            } yield ()
            case _ =>
              F.raiseError(new RuntimeException("Unimplemented declaration type"))
          }.void
        }

        def resolve(publishCommand: PublishCommand): F[List[QueueName]] = resources.get.map(
          _.collect { case (ex, qu, rk) if ex == publishCommand.exchange && rk == publishCommand.routingKey => qu
          })

        override def publisher(): Publisher[F, consume.PublishCommand] = publishCommand =>
          resolve(publishCommand).flatMap(_.traverse { queue =>
            F.delay(sqs.sendMessage(queue.value, publishCommand.body.unmarshal[String].right.get)).void
          }.void)

        val deadLetterPublisher: Publisher[F, consume.PublishCommand] = publisher()

        private val lock = new Object()

        override def registerConsumer(queueName: bucky.QueueName, handler: Handler[F, consume.Delivery], exceptionalAction: consume.ConsumeAction, prefetchCount: Int): Resource[F, Unit] = {
          val await: IO[(Ref[IO, Boolean], Fiber[IO, Unit])] = {
            Ref.of[IO, Boolean](false).flatMap { isStopped =>
              IO.delay(awaitMessages(queueName, handler, isStopped)).start.map { fiber =>
                (isStopped, fiber)
              }
            }
          }

          def cancel(refAndFiber: (Ref[IO, Boolean], Fiber[IO, Unit])): IO[Unit] =
            refAndFiber match {
              case (isStopped, fiber) =>
                val setStopped = IO.delay {
                  lock.synchronized {
                    println("Stopped")
                    isStopped.set(true).unsafeRunSync()
                  }
                }

                setStopped *> fiber.cancel
            }

          Resource.make(await)(cancel).mapK[F](FunctionK.lift[IO, F](F.liftIO)).map(_ => ())
        }

        override def isConnectionOpen: F[Boolean] = ???

        private def awaitMessages(queueName: QueueName, handler: Handler[F, consume.Delivery], isStopped: Ref[IO, Boolean]) = {
          var stopped = false
          while (!stopped) {
            lock.synchronized {
              stopped = isStopped.get.unsafeRunSync()
              if (!stopped) {
                val messageRequest = new ReceiveMessageRequest()
                  .withMaxNumberOfMessages(1)
                  .withQueueUrl(queueName.value)
                val result = sqs.receiveMessage(messageRequest)
                result.getMessages.asScala.headOption.foreach { msg =>
                  val delivery = Delivery(Payload.from[String](msg.getBody), ConsumerTag("foo"), Envelope(0L, redeliver = false, ExchangeName(""), RoutingKey(queueName.value)), MessageProperties.basic)

                  F.toIO[ConsumeAction](handler.apply(delivery)).flatMap {
                    case Ack => F.delay(sqs.deleteMessage(queueName.value, msg.getReceiptHandle))
                    case DeadLetter => deadLetterMappings.get.flatMap{mappings =>
                                          mappings.get(queueName).fold(F.unit)(dlx => deadLetterPublisher.apply(PublishCommand(dlx, ???, ???, ???)))
                    }
                    case _ => ???
                  }.unsafeRunSync()
                }
              }
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

    val queueName = QueueName("bucky-sqs-test--queue-foobar")
    val exchangeName = ExchangeName("bucky-sqs-exchange")
    val routingKey = RoutingKey("rk")


    val declarations = List(
      Queue(queueName),
      Exchange(exchangeName).binding(routingKey -> queueName)
    )

    val clientResource =
      for {
        client <- SqsAmqpClient[IO](builder)
        _ <- Resource.liftF(client.declare(declarations))
        _ <- client.registerConsumer(queueName, handler)
      }
        yield client

    clientResource.use { amqpClient =>
      val publisher = amqpClient.publisher()
      publisher(PublishCommand(exchangeName, routingKey, MessageProperties.basic, Payload.from[String]("hello, world!"))) *> IO.never
    }.as(ExitCode.Success)
  }

}