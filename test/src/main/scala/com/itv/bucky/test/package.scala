package com.itv.bucky

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.itv.bucky.consume._
import cats.implicits._
import cats.effect._
import com.itv.bucky.test.stubs.{RecordingHandler, RecordingRequeueHandler, StubChannel, StubPublisher}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.higherKinds

package object test {
  object Config {
    def empty(timeout: FiniteDuration = 3.seconds): AmqpClientConfig =
      AmqpClientConfig(host = "", port = 0, username = "", password = "", publishingTimeout = timeout)
  }

  object StubChannels {
    def forgiving[F[_]](implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): StubChannel[F] =
      new StubChannel[F] {
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.unit
      }

    def strict[F[_]](implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): StubChannel[F] =
      new StubChannel[F]() {
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.map(F.fromEither(result))(_ => ())
      }

    def publishTimeout[F[_]](implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): StubChannel[F] =
      new StubChannel[F]() {
        override def publish(cmd: PublishCommand): F[Unit] = F.delay {
          pubSeqLock.synchronized(publishSeq = publishSeq + 1)
        }
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.map(F.fromEither(result))(_ => ())
      }

    def allShallAck[F[_]](implicit F: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F]): StubChannel[F] =
      new StubChannel[F]() {
        override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
          F.fromEither(result)
            .flatMap { result =>
              F.ifM[Unit](F.delay(result.forall(_ == Ack)))(F.unit, F.raiseError(new RuntimeException("Not all consumers ack the result.")))
            }
            .void
      }
  }

  object AmqpClientTest {
    def apply: AmqpClientTest = new AmqpClientTest {}
    def apply(implicit executionContext: ExecutionContext, t: Timer[IO], contextShift: ContextShift[IO]): AmqpClientTest = new AmqpClientTest {
      override implicit val ec: ExecutionContext = executionContext
      override implicit val cs: ContextShift[IO] = contextShift
      override implicit val timer: Timer[IO] = t
    }
  }

  trait AmqpClientTest {

    implicit val ec: ExecutionContext = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO]     = IO.timer(ec)

    def client(channel: StubChannel[IO], config: AmqpClientConfig): Resource[IO, AmqpClient[IO]] =
      AmqpClient[IO](
        config,
        Resource.pure[IO, Channel[IO]](channel)
      )

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def client(config: AmqpClientConfig = Config.empty()): Resource[IO, AmqpClient[IO]] =
      clientStrict(config)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param config
      * @return
      */
    def clientAllAck(config: AmqpClientConfig = Config.empty()): Resource[IO, AmqpClient[IO]] =
      client(StubChannels.allShallAck[IO], config)

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def clientStrict(config: AmqpClientConfig = Config.empty()): Resource[IO, AmqpClient[IO]] =
      client(StubChannels.strict[IO], config)

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param config
      * @return
      */
    def clientForgiving(config: AmqpClientConfig = Config.empty()): Resource[IO, AmqpClient[IO]] =
      client(StubChannels.forgiving[IO], config)

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param config
      * @return
      */
    def clientPublishTimeout(config: AmqpClientConfig = Config.empty()): Resource[IO, AmqpClient[IO]] =
      client(StubChannels.publishTimeout[IO], config)

    def runAmqpTest(clientResource: Resource[IO, AmqpClient[IO]])(test: AmqpClient[IO] => IO[Unit]): Unit =
      clientResource.map(_.withLogging()).use(test).unsafeRunSync()

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTest(test: AmqpClient[IO] => IO[Unit]): Unit =
      runAmqpTestStrict(test)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param test
      */
    def runAmqpTestAllAck(test: AmqpClient[IO] => IO[Unit]): Unit =
      runAmqpTest(clientAllAck())(test)

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTestStrict(test: AmqpClient[IO] => IO[Unit]): Unit =
      runAmqpTest(clientStrict())(test)

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param test
      */
    def runAmqpTestForgiving(test: AmqpClient[IO] => IO[Unit]): Unit =
      runAmqpTest(clientForgiving())(test)

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param test
      */
    def runAmqpTestPublishTimeout(test: AmqpClient[IO] => IO[Unit]): Unit =
      runAmqpTest(clientPublishTimeout())(test)

  }

  object StubHandlers {
    def ackHandler[F[_], T](implicit F: Sync[F]): RecordingHandler[F, T]                   = new RecordingHandler[F, T](_ => F.delay(Ack))
    def deadLetterHandler[F[_], T](implicit F: Sync[F]): RecordingHandler[F, T]            = new RecordingHandler[F, T](_ => F.delay(DeadLetter))
    def requeueRequeueHandler[F[_], T](implicit F: Sync[F]): RecordingRequeueHandler[F, T] = new RecordingRequeueHandler[F, T](_ => F.delay(Requeue))
    def ackRequeueHandler[F[_], T](implicit F: Sync[F]): RecordingRequeueHandler[F, T]     = new RecordingRequeueHandler[F, T](_ => F.delay(Ack))
    def deadletterRequeueHandler[F[_], T](implicit F: Sync[F]): RecordingRequeueHandler[F, T] =
      new RecordingRequeueHandler[F, T](_ => F.delay(DeadLetter))
    def recordingHandler[F[_], T](handler: Handler[F, T])(implicit F: Sync[F]): RecordingHandler[F, T] = new RecordingHandler[F, T](handler)
  }

  object StubPublishers {
    def stubPublisher[F[_], T](implicit F: Sync[F]): StubPublisher[F, T] = new StubPublisher[F, T]()
  }
}
