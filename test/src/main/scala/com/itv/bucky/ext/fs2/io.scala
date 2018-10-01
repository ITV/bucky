package com.itv.bucky.ext.fs2
import _root_.fs2._
import cats.data.EitherT
import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.decl._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import cats.effect._
import cats.implicits._
import com.itv.bucky.ext.fs2.ConsumeActionResult.UnableToBeConsumed
import com.typesafe.scalalogging.StrictLogging

protected[fs2] object io extends StrictLogging {

  import _root_.fs2.async

  def apply(maxRetries: Int = 10)(implicit executionContext: ExecutionContext,
                                  scheduler: Scheduler,
                                  idMonad: Monad[Id],
                                  ioMonadError: MonadError[IO, Throwable],
                                  F: Sync[IO]): IO[MemoryAmqpSimulator[IO]] =
    for {
      originalMessages <- async.refOf(List.empty[Message])
      bindings         <- async.refOf(List.empty[Binding])
      consumers        <- async.refOf(Map.empty[QueueName, Handler[IO, Delivery]])
      deliveryTagInc   <- async.signalOf[IO, Int](0)
    } yield
      new MemoryAmqpSimulator[IO] {

        override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError
        override implicit def monad: Monad[Id]                       = idMonad

        override def waitForMessagesToBeProcessed(timeout: FiniteDuration): IO[List[ConsumeActionResult]] =
          originalMessages.get.flatMap(
            _.traverse(
              m =>
                m.promise
                  .timedGet(timeout, scheduler)
                  .map(_.getOrElse(ConsumeActionResult.TimeoutToPublish(m.publishCommand, timeout)))))

        override def publish(publishCommand: PublishCommand): IO[async.Promise[IO, ConsumeActionResult]] =
          for {
            promise <- async.promise[IO, ConsumeActionResult]
            message = Message(publishCommand, promise)
            _ <- originalMessages.modify(_.+:(message))
            _ <- processNext(Message.Data(message, 0, Message.Source.Original))
          } yield message.promise

        override def publisher(timeout: Duration): Publisher[IO, PublishCommand] =
          publishCommand => publish(publishCommand).void
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

        private def bindingFor(messageData: Message.Data): List[Binding] => Option[Binding] =
          _.find(
            b =>
              messageData.message.publishCommand.routingKey == b.routingKey &&
                messageData.message.publishCommand.exchange == b.exchangeName)

        private def processNext(messageData: Message.Data): IO[Unit] =
          for {
            retryOrHandler <- handleFor(messageData)
            _ <- retryOrHandler.fold(
              retryOrFail,
              handle(messageData, _)
            )
          } yield ()

        private def retryOrFail(messageData: Message.Data): IO[Unit] =
          if (messageData.retries > maxRetries)
            messageData.source match {
              case Message.Source.Original =>
                //FIXME Review this case
                messageData.message.promise
                  .complete(UnableToBeConsumed(messageData.message.publishCommand, "Not sure!!!!"))

              case Message.Source.NoBindingFound =>
                messageData.message.promise
                  .complete(messageData.message.publishCommand.notBindingFound)

              case Message.Source.NoConsumerFound =>
                messageData.message.promise
                  .complete(messageData.message.publishCommand.notConsumerFound)

            } else
            processNext(messageData)

        private def handleFor(messageData: Message.Data): IO[Either[Message.Data, Handler[IO, Delivery]]] =
          (for {
            binding <- EitherT(
              bindings.get
                .map(bindingFor(messageData))
                .map(_.fold(Message
                  .noBindingFound(messageData)
                  .asLeft[Binding])(_.asRight[Message.Data])))
            result <- EitherT(
              consumers.get.map(
                _.get(binding.queueName)
                  .fold(
                    Message
                      .noConsumerFound(messageData)
                      .asLeft[Handler[IO, Delivery]]
                  )(h => h.asRight[Message.Data])))
          } yield result).value

        private def handle(messageData: Message.Data, h: Handler[IO, Delivery]): IO[Unit] =
          for {
            next <- deliveryTagInc.modify(_ + 1)
            _ <- async.fork(
              h(Delivery(
                messageData.message.publishCommand.body,
                ConsumerTag("ctag"),
                Envelope(next.now,
                         redeliver = false,
                         messageData.message.publishCommand.exchange,
                         messageData.message.publishCommand.routingKey),
                messageData.message.publishCommand.basicProperties
              )).flatMap(completePromise(_, messageData)))
          } yield ()

        private def completePromise(consumeAction: ConsumeAction, messageData: Message.Data): IO[Unit] =
          for {
            _ <- messageData.message.promise.complete(ConsumeActionResult.Consumed(consumeAction))
            _ <- IO { logger.info(s"$consumeAction for ${messageData.message.show}") }
          } yield ()
        override def publishAndWait(publishCommand: PublishCommand, timeout: FiniteDuration): IO[ConsumeActionResult] =
          publish(publishCommand).flatMap(
            _.timedGet(timeout, scheduler).map(_.getOrElse(publishCommand.timeout(timeout))))
      }

}
