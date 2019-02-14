package com.itv.bucky.ext.fs2
import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object old {
  import _root_.fs2._

  @deprecated("Please use com.itv.bucky.ext.fs2.supersync.SuperSyncSimulator instead - other simulators have known concurrency issues that will make your tests flakey", "2019-02-13")
  def rabbitSimulator(implicit executionContext: ExecutionContext,
                      ioMonadError: MonadError[IO, Throwable],
                      futureMonad: MonadError[Future, Throwable]): Fs2AmqpSimulator =
    new Fs2AmqpSimulator with StrictLogging {

      override implicit def monad: Monad[Id] = Monad.idMonad

      override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError

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

}
