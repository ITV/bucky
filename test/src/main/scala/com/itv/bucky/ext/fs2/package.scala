package com.itv.bucky.ext

import _root_.fs2._
import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.decl._

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Assertion

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

package object fs2 {

  type Fs2AmqpSimulator = AmqpSimulator[Id, IO, Throwable, Stream[IO, Unit]]

  def rabbitSimulator(implicit executionContext: ExecutionContext,
                      ioMonadError: MonadError[IO, Throwable],
                      futureMonad: MonadError[Future, Throwable]): Fs2AmqpSimulator =
    new Fs2AmqpSimulator with StrictLogging {

      override implicit def monad: Monad[Id] = Monad.idMonad

      override implicit def effectMonad = ioMonadError

      val rabbitSimulator = new RabbitSimulator[Id]()(
        Monad.idMonad,
        futureMonad,
        executionContext
      )

      override def publisher(timeout: Duration): Id[Publisher[IO, PublishCommand]] =
        cmd => IO.fromFuture(IO(rabbitSimulator.publisher(timeout)(cmd)))

      override def consumer(queueName: QueueName,
                            handler: Handler[IO, Delivery],
                            exceptionalAction: ConsumeAction = DeadLetter,
                            prefetchCount: Int = 0): Id[Stream[IO, Unit]] =
        Stream.eval(IO {
          logger.info(s"Create consumer for $queueName")
          rabbitSimulator.consumer(queueName,
                                   (delivery: Delivery) => handler(delivery).unsafeToFuture(),
                                   exceptionalAction,
                                   prefetchCount)

        })

      def waitForMessagesToBeProcessed(): IO[Iterable[ConsumeAction]] =
        IO.fromFuture(IO(rabbitSimulator.waitForMessagesToBeProcessed()))

      def existsConsumer(queueName: QueueName): Boolean = rabbitSimulator.existsConsumer(queueName)

      def watchQueue(queueName: QueueName): ListBuffer[Delivery] = rabbitSimulator.watchQueue(queueName)

      def isDefinedAt(publishCommand: PublishCommand): Boolean = rabbitSimulator.isDefinedAt(publishCommand)

      def queueNameFor(publishCommand: PublishCommand): QueueName = rabbitSimulator.queueNameFor(publishCommand)

      def publish(publishCommand: PublishCommand): IO[ConsumeAction] =
        IO.fromFuture(IO(rabbitSimulator.publish(publishCommand)))

      override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = rabbitSimulator.performOps(thunk)

      override def estimatedMessageCount(queueName: QueueName): Try[Int] =
        //FIXME rabbitSimulator.estimatedMessageCount(queueName)
        Try(1)

    }

  implicit class RabbitSimulatorExt(amqpClient: AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]) extends StrictLogging {

    def consume(exchangeName: ExchangeName,
                routingKey: RoutingKey,
                queueName: QueueName = QueueName(s"queue-${Random.nextInt(1000)}"))(
        implicit executionContext: ExecutionContext,
        ioMonadError: MonadError[IO, Throwable]): Stream[IO, ListBuffer[Delivery]] = {
      val stubConsumeHandler = new StubConsumeHandler[IO, Delivery]()(ioMonadError)

      Stream
        .eval(IO(stubConsumeHandler.receivedMessages))
        .concurrently(
          Stream
            .eval(IO {
              val testDeclaration = List(
                Queue(queueName),
                Exchange(exchangeName, exchangeType = Topic)
                  .binding(routingKey -> queueName)
              )
              logger.info(s"Create consumer for: $exchangeName -> $routingKey -> $queueName")
              DeclarationExecutor(testDeclaration, amqpClient)
            })
            .flatMap(_ => amqpClient.consumer(queueName, stubConsumeHandler)))
    }
  }

  def withSimulator[P](declarations: Iterable[Declaration] = List.empty)(ports: Fs2AmqpSimulator => Stream[IO, P])(
      test: P => IO[Assertion])(implicit executionContext: ExecutionContext,
                                ioMonadError: MonadError[IO, Throwable],
                                futureMonad: MonadError[Future, Throwable]): Unit = {
    val amqpClient = rabbitSimulator
    val p = for {
      halted <- Stream.eval(async.signalOf[IO, Boolean](false))
      _      <- Stream.eval(IO(DeclarationExecutor(declarations, amqpClient)))
      ports  <- ports(amqpClient).interruptWhen(halted)
      _      <- Stream.eval(test(ports))
      _      <- Stream.eval(halted.set(true))
    } yield ()
    p.compile.last.unsafeRunSync()
  }
}
