package com.itv.bucky.fs2

import cats.effect.{ContextShift, IO, Timer}
import com.itv.bucky.Monad.Id
import com.itv.bucky.{AmqpClient, AmqpClientConfig}
import com.itv.bucky.fs2.utils.{IOEffectMonad, IOEffectVerification}
import com.itv.bucky.suite.NetworkRecoveryIntegrationTest
import com.itv.lifecycle.Lifecycle
import fs2._
import org.scalatest.Ignore

import scala.concurrent.duration.FiniteDuration

@Ignore
class IONetworkRecoveryIntegrationTest
    extends NetworkRecoveryIntegrationTest[IO, Throwable, IOConsumer]
    with IOEffectVerification
    with IOEffectMonad {
  import com.itv.bucky.future.SameThreadExecutionContext.implicitly

  implicit val contextShift: ContextShift[IO] = IO.contextShift(implicitly)
  implicit val timer: Timer[IO] = IO.timer(implicitly)

  override def schedule(f: => Unit, duration: FiniteDuration): Unit =
    Stream.fixedDelay(duration).map(_ => f).head.compile.drain.unsafeRunAsync(_ => ())

  override def buildLifecycle(config: AmqpClientConfig): Lifecycle[AmqpClient[Id, IO, Throwable, IOConsumer]] =
    IOAmqpClient.lifecycle(config)

  override def defaultAmqpClientConfig: AmqpClientConfig = utils.config

  override def executeConsumer(c: IOConsumer): Unit = c.compile.drain.unsafeRunAsync(_ => ())
}
