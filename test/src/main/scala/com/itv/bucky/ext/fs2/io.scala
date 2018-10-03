package com.itv.bucky.ext.fs2

import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.decl._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging

import _root_.fs2._
import _root_.fs2.async

import cats.data.EitherT
import cats.effect.IO
import cats.effect._
import cats.implicits._

protected[fs2] object io extends StrictLogging {

  import Message._

  def apply(config: MemoryAmqpSimulator.Config)(implicit executionContext: ExecutionContext,
                                                scheduler: Scheduler,
                                                idMonad: Monad[Id],
                                                ioMonadError: MonadError[IO, Throwable],
                                                F: Sync[IO]): IO[MemoryAmqpSimulator[IO]] =
    for {
      sourceMessages <- async.refOf(List.empty[Message.Source])
      bindings       <- async.refOf(List.empty[Binding])
      consumers      <- async.refOf(Map.empty[QueueName, Handler[IO, Delivery]])
      deliveryTagInc <- async.signalOf[IO, Int](0)
    } yield
      new MemoryAmqpSimulator[IO] {

        override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError
        override implicit def monad: Monad[Id]                       = idMonad

        override def publish(publishCommand: PublishCommand): IO[async.Promise[IO, ConsumeActionResult]] =
          for {
            promise <- async.promise[IO, ConsumeActionResult]
            message = Message.Source(publishCommand, promise)
            _ <- sourceMessages.modify(_.+:(message))
            _ <- IO { logger.info(s"Message published: ${publishCommand.show}") }
            _ <- processNext(message)
          } yield promise

        override def publishAndWait(publishCommand: PublishCommand, timeout: FiniteDuration): IO[ConsumeActionResult] =
          publish(publishCommand).flatMap(
            completePromiseOrTimeout(_, publishCommand, timeout)
          )

        override def waitForMessagesToBeProcessed(timeout: FiniteDuration): IO[List[ConsumeActionResult]] =
          sourceMessages.get.flatMap(
            _.traverse(
              message => completePromiseOrTimeout(message.promise, message.publishCommand, timeout)
            ))

        override def publisher(timeout: Duration): Publisher[IO, PublishCommand] =
          publish(_).void

        override def consumer(queueName: QueueName,
                              handler: Handler[IO, Delivery],
                              exceptionalAction: ConsumeAction,
                              prefetchCount: Int): Stream[IO, Unit] =
          Stream
            .eval(consumers.modify(_ + (queueName -> handler)).void)
            .observe1(_ => IO { logger.info(s"Consumer ready in $queueName") })

        override def performOps(thunk: AmqpOps => Try[Unit]): Try[Unit] =
          thunk(amqpOpsFor(x => Try { addBinding(x).unsafeRunSync() }))

        override def estimatedMessageCount(queueName: QueueName): Try[Int] = Try(0)

        private def addBinding(binding: Binding) = bindings.modify(_.+:(binding))

        private def bindingFor(message: Message): List[Binding] => Option[Binding] = _.find { b =>
          val publishCommand = Message.sourceFrom(message).publishCommand
          publishCommand.routingKey == b.routingKey &&
          publishCommand.exchange == b.exchangeName
        }

        private def processNext(message: Message): IO[Unit] =
          for {
            retryOrHandler <- handlerFor(message)
            _ <- retryOrHandler.fold(
              retryOrFail,
              handle(message, _)
            )
          } yield ()

        private def handlerFor(message: Message): IO[Either[Message.Retry, Handler[IO, Delivery]]] =
          (for {
            binding <- EitherT(
              bindings.get
                .map(bindingFor(message))
                .map(_.fold(Message
                  .noBindingFound(message)
                  .asLeft[Binding])(_.asRight[Message.Retry])))
            result <- EitherT(
              consumers.get.map(
                _.get(binding.queueName)
                  .fold(
                    Message
                      .noConsumerFound(message)
                      .asLeft[Handler[IO, Delivery]]
                  )(h => h.asRight[Message.Retry])))
          } yield result).value

        private def retryOrFail(message: Message.Retry): IO[Unit] =
          if (message.retries > config.retryPolicy.total)
            completePromise(
              message,
              message.issue match {
                case Message.Issue.NoBindingFound =>
                  message.source.publishCommand.notBindingFound(config.retryPolicy)
                case Message.Issue.NoConsumerFound =>
                  message.source.publishCommand.notConsumerFound(config.retryPolicy)
              }
            )
          else
            (scheduler.sleep_[IO](config.retryPolicy.sleep) ++ Stream.eval(processNext(message))).compile.last.void

        private def handle(message: Message, handler: Handler[IO, Delivery]): IO[Unit] =
          for {
            next <- deliveryTagInc.modify(_ + 1)
            _ <- async.fork(
              handler(deliveryFor(next, Message.sourceFrom(message)))
                .flatMap(x => completePromise(message, x.result)))
          } yield ()

        private def deliveryFor(next: async.Ref.Change[Int], source: Message.Source): Delivery =
          Delivery(
            source.publishCommand.body,
            ConsumerTag("ctag"),
            Envelope(next.now, redeliver = false, source.publishCommand.exchange, source.publishCommand.routingKey),
            source.publishCommand.basicProperties
          )

        private def completePromiseOrTimeout(promise: async.Promise[IO, ConsumeActionResult],
                                             publishCommand: PublishCommand,
                                             timeout: FiniteDuration): IO[ConsumeActionResult] =
          promise
            .timedGet(timeout, scheduler)
            .flatMap(
              _.fold(timeoutFor(publishCommand, timeout))(
                IO.pure
              ))

        private def completePromise(message: Message, consumeActionResult: ConsumeActionResult): IO[Unit] =
          for {
            _ <- Message.sourceFrom(message).promise.complete(consumeActionResult)
            _ <- showResult(Message.sourceFrom(message).publishCommand, consumeActionResult)
          } yield ()

        private def timeoutFor(publishCommand: PublishCommand, after: FiniteDuration): IO[ConsumeActionResult] = {
          val result = publishCommand.timeout(after)
          showResult(publishCommand, result).map(_ => result)
        }
        private def showResult(publishCommand: PublishCommand, consumeActionResult: ConsumeActionResult): IO[Unit] =
          IO {
            logger.info(s"${consumeActionResult.show} for ${publishCommand.show}")
          }

      }

}
