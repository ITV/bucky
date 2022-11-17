package com.itv.bucky

import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import cats.effect._
import cats.implicits._
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.test.stubs.RecordingHandler.{ConsumeActionBufferRef, ListBufferRef, RequeueConsumeActionBufferRef}
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
    def forgiving[F[_]](implicit F: Async[F], t: Temporal[F]): F[StubChannel[F]] = {
      Ref[F].of(0L).map { publishSeq =>
        new StubChannel[F](publishSeq) {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.unit
        }
      }
    }

    def strict[F[_]](implicit F: Async[F], t: Temporal[F]): F[StubChannel[F]] = {
      Ref[F].of(0L).map { publishSeq =>
        new StubChannel[F](publishSeq) {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.map(F.fromEither(result))(_ => ())
        }
      }
    }

    def publishNoAck[F[_]](publishSeq: Ref[F, Long],
                           publishSleepDuration: FiniteDuration = 0.seconds)(implicit F: Async[F], t: Temporal[F]): F[StubChannel[F]] = {
      F.pure {
        new StubChannel[F](publishSeq) {
          override def publish(sequenceNumber: Long, cmd: PublishCommand): F[Unit] = {
            val log = if (publishSleepDuration == 0.seconds) F.unit else F.delay(logger.info(s"Sleeping $publishSleepDuration before publishing"))

            log *>
              F.sleep(publishSleepDuration) *>
              publishSeq.update(_ => sequenceNumber + 1)
          }

          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.map(F.fromEither(result))(_ => ())
        }
      }
    }

    def allShallAck[F[_]](implicit F: Async[F], t: Temporal[F]): F[StubChannel[F]] = {
      Ref[F].of(0L).map { publishSeq =>
        new StubChannel[F](publishSeq) {
          override def handlePublishHandlersResult(result: Either[Throwable, List[consume.ConsumeAction]]): F[Unit] =
            F.fromEither(result)
              .flatMap { result =>
                F.ifM[Unit](F.delay(result.forall(_ == Ack)))(F.unit, F.raiseError(new RuntimeException("Not all consumers ack the result.")))
              }
              .void
        }
      }
    }
  }

  object IOAmqpClientTest {
    def apply(implicit executionContext: ExecutionContext): AmqpClientTest[IO] = {
      new AmqpClientTest[IO] {
        implicit val ec: ExecutionContext = executionContext
      }
    }
  }

  trait IOAmqpClientTest extends AmqpClientTest[IO] {
    val globalExecutionContext: ExecutionContext = ExecutionContext.Implicits.global
    override implicit val ec: ExecutionContext = globalExecutionContext
  }

  object AmqpClientTest {
    def apply[F[_]](implicit async: Async[F], executionContext: ExecutionContext): AmqpClientTest[F] =
      new AmqpClientTest[F]() {
        override implicit val ec: ExecutionContext = executionContext
      }
  }

  trait AmqpClientTest[F[_]] {

    implicit val ec: ExecutionContext

    def client(channel: StubChannel[F], config: AmqpClientConfig)(implicit async: Async[F]): Resource[F, AmqpClient[F]] = {
      Dispatcher[F].flatMap { dispatcher =>
        AmqpClient[F](
          config,
          () => Resource.pure[F, Channel[F]](channel),
          Resource.pure[F, Channel[F]](channel),
          dispatcher
        )
      }
    }

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def client(config: AmqpClientConfig = Config.empty())(implicit async: Async[F]): Resource[F, AmqpClient[F]] =
      clientStrict(config)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param config
      * @return
      */
    def clientAllAck(config: AmqpClientConfig = Config.empty())(implicit async: Async[F]): Resource[F, AmqpClient[F]] =
      Resource.eval(StubChannels.allShallAck[F]).flatMap(ch => client(ch, config))

    /**
      * A publish will fail if any handler throws an exception
      * @param config
      * @return
      */
    def clientStrict(config: AmqpClientConfig = Config.empty())(implicit async: Async[F]): Resource[F, AmqpClient[F]] =
      Resource.eval(StubChannels.strict[F]).flatMap(ch => client(ch, config))

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param config
      * @return
      */
    def clientForgiving(config: AmqpClientConfig = Config.empty())(implicit async: Async[F]): Resource[F, AmqpClient[F]] =
      Resource.eval(StubChannels.forgiving[F]).flatMap(ch => client(ch, config))

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param config
      * @return
      */
    def clientPublishTimeout(config: AmqpClientConfig = Config.empty())(implicit async: Async[F]): Resource[F, AmqpClient[F]] = {
      for {
        publishSeqRef <- Resource.eval(Ref[F].of(0L))
        channel <- Resource.eval(StubChannels.publishNoAck[F](publishSeqRef))
        c <- client(channel, config)
      } yield c
    }

    def runAmqpTest(clientResource: Resource[F, AmqpClient[F]])(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      clientResource.map(_.withLogging()).use(test)

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTest(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      runAmqpTestStrict(test)

    /**
      * For a publish to succeed all handlers must respond with Ack
      * @param test
      */
    def runAmqpTestAllAck(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      runAmqpTest(clientAllAck())(test)

    /**
      * A publish will fail if any handler throws an exception
      * @param test
      */
    def runAmqpTestStrict(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      runAmqpTest(clientStrict())(test)

    /**
      * Publishes always succeed, even if a handler throws an exception or does not Ack
      * @param test
      */
    def runAmqpTestForgiving(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      runAmqpTest(clientForgiving())(test)

    /**
      * Every attempt to publish will result in a timeout (after the time specified in config)
      * @param test
      */
    def runAmqpTestPublishTimeout(test: AmqpClient[F] => F[Unit])(implicit async: Async[F]): F[Unit] =
      runAmqpTest(clientPublishTimeout())(test)

  }

  object StubHandlers {

    def ackHandler[F[_], T](ref: ConsumeActionBufferRef[F,T])(implicit F: Sync[F]): RecordingHandler[F, T]                   = new RecordingHandler[F, T](_ => F.delay(Ack), ref)
    def deadLetterHandler[F[_], T](ref: ConsumeActionBufferRef[F,T])(implicit F: Sync[F]): RecordingHandler[F, T]            = new RecordingHandler[F, T](_ => F.delay(DeadLetter), ref)
    def requeueRequeueHandler[F[_], T](ref: RequeueConsumeActionBufferRef[F,T])(implicit F: Sync[F]): RecordingRequeueHandler[F, T] = new RecordingRequeueHandler[F, T](_ => F.delay(Requeue), ref)

    def recordingHandler[F[_], T](handler: Handler[F, T],
                                  ref: ConsumeActionBufferRef[F,T])(implicit F: Sync[F]): RecordingHandler[F, T] = new RecordingHandler[F, T](handler, ref)
  }

  object StubPublishers {
    def stubPublisher[F[_], T](implicit F: Sync[F]): StubPublisher[F, T] = new StubPublisher[F, T]()
  }
}
