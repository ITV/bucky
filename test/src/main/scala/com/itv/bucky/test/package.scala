package com.itv.bucky

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.itv.bucky.consume._
import cats.implicits._
import cats.effect._
import com.itv.bucky.test.stubs.{RecordingHandler, StubChannel, StubPublisher}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object test {
  object Config {
    def empty(timeout: FiniteDuration): AmqpClientConfig =
      AmqpClientConfig(host = "", port = 0, username = "", password = "", publishingTimeout = timeout)
  }

  object TestAmqpClient {
    def forgivingSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): Resource[F, AmqpClient[F]] =
      AmqpClient.apply[F](config, Resource.pure[F, Channel[F]](new StubChannel[F]() {
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.unit
      }))

    def strictSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): Resource[F, AmqpClient[F]] =
      AmqpClient.apply[F](
        config,
        Resource.pure[F, Channel[F]](new StubChannel[F]() {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.fromEither(result).void
        })
      )

    def allShallAckSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): Resource[F, AmqpClient[F]] =
      AmqpClient.apply[F](
        config,
        Resource.pure[F, Channel[F]](new StubChannel[F]() {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.fromEither(result)
              .flatMap { result =>
                F.ifM[Unit](F.delay(result.forall(_ == Ack)))(F.unit, F.raiseError(new RuntimeException("Not all consumers ack the result.")))
              }
              .void
        }
      ))
    def superSyncSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): Resource[F, AmqpClient[F]] =
      forgivingSimulator(config)
  }

  object StubHandlers {
    def ackHandler[F[_], T](implicit F: Sync[F]): RecordingHandler[F, T]                               = new RecordingHandler[F, T](_ => F.delay(Ack))
    def deadLetterHandler[F[_], T](implicit F: Sync[F]): RecordingHandler[F, T]                        = new RecordingHandler[F, T](_ => F.delay(DeadLetter))
    def requeueHandler[F[_], T](implicit F: Sync[F]): RecordingHandler[F, T]                           = new RecordingHandler[F, T](_ => F.delay(RequeueImmediately))
    def recordingHandler[F[_], T](handler: Handler[F, T])(implicit F: Sync[F]): RecordingHandler[F, T] = new RecordingHandler[F, T](handler)
  }

  object Publishers {
    def stubPublisher[F[_], T](implicit F: Sync[F]): StubPublisher[F, T] = new StubPublisher[F, T]()
  }
}
