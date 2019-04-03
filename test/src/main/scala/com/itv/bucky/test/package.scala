package com.itv.bucky

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import com.itv.bucky.consume._
import cats._
import cats.implicits._
import com.itv.bucky.test.stubs.{StubChannel, StubHandler, StubPublisher}
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object test {
  object Config {
    def empty(timeout: FiniteDuration): AmqpClientConfig =
      AmqpClientConfig(host = "", port = 0, username = "", password = "", publishingTimeout = timeout)
  }

  object TestAmqpClient {
    def forgivingSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): F[AmqpClient[F]] =
      AmqpClient.apply[F](config, new StubChannel[F]() {
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.unit
      })

    def strictSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): F[AmqpClient[F]] =
      AmqpClient.apply[F](
        config,
        new StubChannel[F]() {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.fromEither(result).void
        }
      )

    def allShallAckSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): F[AmqpClient[F]] =
      AmqpClient.apply[F](
        config,
        new StubChannel[F]() {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.fromEither(result)
              .flatMap { result =>
                F.ifM[Unit](F.delay(result.forall(_ == Ack)))(F.unit, F.raiseError(new RuntimeException("Not all consumers ack the result.")))
              }
              .void
        }
      )
    def superSyncSimulator[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): F[AmqpClient[F]] =
      forgivingSimulator(config)
  }

  object Consumer {
    def ackConsumer[F[_], T](implicit F: Sync[F]): StubHandler[F, T]        = new StubHandler[F, T](Ack)
    def deadLetterConsumer[F[_], T](implicit F: Sync[F]): StubHandler[F, T] = new StubHandler[F, T](DeadLetter)
    def requeueConsumer[F[_], T](implicit F: Sync[F]): StubHandler[F, T]    = new StubHandler[F, T](RequeueImmediately)
  }

  object Publisher {
    def stubHandler[F[_], T](implicit F: Sync[F]): StubPublisher[F, T] = new StubPublisher[F, T]()
  }

}
