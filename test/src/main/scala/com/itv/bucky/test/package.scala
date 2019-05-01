package com.itv.bucky

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import com.itv.bucky.consume._
import cats.implicits._
import cats.effect._
import com.itv.bucky.test.stubs.{RecordingHandler, RecordingRequeueHandler, StubChannel, StubPublisher}
import cats.effect.implicits._
import com.itv.bucky.test.AmqpClientTest

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

  object IOAmqpClientTest {
    def apply(executionContext: ExecutionContext, t: Timer[IO], cs: ContextShift[IO]): AmqpClientTest[IO] =
      new AmqpClientTest[IO] {
        implicit val ec: ExecutionContext = executionContext
        override implicit val timer: Timer[IO] = IO.timer(executionContext)
        override implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
        override implicit val F: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)
      }
  }

  trait IOAmqpClientTest extends AmqpClientTest[IO] {
    val globalExecutionContext: ExecutionContext = ExecutionContext.Implicits.global
    override implicit val ec: ExecutionContext = globalExecutionContext
    override implicit val timer: Timer[IO] = IO.timer(globalExecutionContext)
    override implicit val contextShift: ContextShift[IO] = IO.contextShift(globalExecutionContext)
    override implicit val F: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)
  }

  object AmqpClientTest {
    def apply[F[_]](implicit concurrentEffect: ConcurrentEffect[F], t: Timer[F], cs: ContextShift[F], executionContext: ExecutionContext): AmqpClientTest[F] =
      new AmqpClientTest[F]() {
        override implicit val F: ConcurrentEffect[F] = concurrentEffect
        override implicit val timer: Timer[F] = t
        override implicit val contextShift: ContextShift[F] = cs
        override implicit val ec: ExecutionContext = executionContext
      }
  }

  trait AmqpClientTest[F[_]] {

    implicit val F: ConcurrentEffect[F]
    implicit val timer: Timer[F]
    implicit val contextShift: ContextShift[F]
    implicit val ec: ExecutionContext

    def client(channel: StubChannel[F], config: AmqpClientConfig): Resource[F, AmqpClient[F]] =
      AmqpClient[F](
        config,
        () => Resource.pure[F, Channel[F]](channel),
        Resource.pure[F, Channel[F]](channel)
      )

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def client(config: AmqpClientConfig = Config.empty()): Resource[F, AmqpClient[F]] =
      clientStrict(config)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param config
      * @return
      */
    def clientAllAck(config: AmqpClientConfig = Config.empty()): Resource[F, AmqpClient[F]] =
      client(StubChannels.allShallAck[F], config)

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def clientStrict(config: AmqpClientConfig = Config.empty()): Resource[F, AmqpClient[F]] =
      client(StubChannels.strict[F], config)

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param config
      * @return
      */
    def clientForgiving(config: AmqpClientConfig = Config.empty()): Resource[F, AmqpClient[F]] =
      client(StubChannels.forgiving[F], config)

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param config
      * @return
      */
    def clientPublishTimeout(config: AmqpClientConfig = Config.empty()): Resource[F, AmqpClient[F]] =
      client(StubChannels.publishTimeout[F], config)

    def runAmqpTest(clientResource: Resource[F, AmqpClient[F]])(test: AmqpClient[F] => F[Unit]): Unit =
      F.toIO(clientResource.map(_.withLogging()).use(test)).unsafeRunSync()

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTest(test: AmqpClient[F] => F[Unit]): Unit =
      runAmqpTestStrict(test)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param test
      */
    def runAmqpTestAllAck(test: AmqpClient[F] => F[Unit]): Unit =
      runAmqpTest(clientAllAck())(test)

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTestStrict(test: AmqpClient[F] => F[Unit]): Unit =
      runAmqpTest(clientStrict())(test)

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param test
      */
    def runAmqpTestForgiving(test: AmqpClient[F] => F[Unit]): Unit =
      runAmqpTest(clientForgiving())(test)

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param test
      */
    def runAmqpTestPublishTimeout(test: AmqpClient[F] => F[Unit]): Unit =
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
