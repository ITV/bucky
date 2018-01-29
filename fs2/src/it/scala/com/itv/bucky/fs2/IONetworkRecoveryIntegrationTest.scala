package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.Monad.Id
import com.itv.bucky.{AmqpClient, AmqpClientConfig}
import com.itv.bucky.fs2.utils.{IOEffectMonad, IOEffectVerification}
import com.itv.bucky.suite.NetworkRecoveryIntegrationTest
import com.itv.lifecycle.Lifecycle
import fs2._

import scala.concurrent.duration.FiniteDuration

class IONetworkRecoveryIntegrationTest
    extends NetworkRecoveryIntegrationTest[IO, Throwable, IOConsumer]
    with IOEffectVerification
    with IOEffectMonad {
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  override def schedule(f: => Unit, duration: FiniteDuration): Unit =
    Scheduler[IO](corePoolSize = 1).flatMap(_.delay(Stream.eval(IO(f)), duration)).compile.drain.unsafeRunAsync { _ =>
      ()
    }

  override def buildLifecycle(config: AmqpClientConfig): Lifecycle[AmqpClient[Id, IO, Throwable, IOConsumer]] =
    IOAmqpClient.lifecycle(config)

  override def defaultAmqpClientConfig: AmqpClientConfig = utils.config

  override def executeConsumer(c: IOConsumer): Unit = c.compile.drain.unsafeRunAsync(_ => ())
}
