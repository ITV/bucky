package com.itv.bucky.publish

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ConcurrentEffect, Resource}
import com.rabbitmq.client.AMQP.{Basic, Channel => RabbitChannel}
import com.rabbitmq.client.{Method, ShutdownListener, ShutdownSignalException}
import cats.implicits._
import cats.effect.implicits._
import com.itv.bucky.Channel

import scala.collection.immutable.TreeMap
import scala.language.higherKinds

case class PublishChannelManager[F[_]](
                                        buildChannel: F[(Channel[F], F[Unit])],
                                        publishChannelRef: Ref[F, (Channel[F], F[Unit])],
                                        pendingConfirmListener: PendingConfirmListener[F]
                                      )(implicit F: ConcurrentEffect[F]) extends ShutdownListener {
  lazy val basicPublish: Method = (new Basic.Publish.Builder()).build()

  private def newPublishChannel: F[Unit] =
    buildChannel.flatMap { case (channel, closer) =>
      for {
        _ <- channel.addShutdownListener(this)
        _ <- publishChannelRef.set((channel, closer))
      }
        yield ()
    }

  def close: F[Unit] =
    publishChannelRef.get.flatMap {
      case (_, closer) =>
        closer
    }

  override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
    cause.getReason match {
      case reason: RabbitChannel.Close if reason.getClassId == basicPublish.protocolClassId() && reason.getMethodId == basicPublish.protocolMethodId() =>
        (for {
          _ <- pendingConfirmListener.abortAll(cause)
          _ <- newPublishChannel
        }
          yield ()).toIO.unsafeRunSync()
      case _ => ()
    }
  }
}

object PublishChannelManager {

  private[this] def newPublishChannel[F[_]](
                                             channelResource: Resource[F, Channel[F]],
                                             pendingConfirmListener: PendingConfirmListener[F]
                                           )(implicit F: ConcurrentEffect[F]): F[(Channel[F], F[Unit])] =
    channelResource.allocated.flatMap { case (channel, closer) =>
      for {
        _ <- channel.confirmSelect
        _ <- channel.addConfirmListener(pendingConfirmListener)
      }
        yield (channel, closer)
    }

  def apply[F[_]](buildChannel: Resource[F, Channel[F]])(implicit F: ConcurrentEffect[F]): F[PublishChannelManager[F]] =
    for {
      pendingConfirmationnsRef <- Ref.of[F, TreeMap[Long, Deferred[F, ConfirmationResult]]](TreeMap.empty)
      pendingConfirmationsListener  = PendingConfirmListener(pendingConfirmationnsRef)
      channelBuilder = newPublishChannel(buildChannel, pendingConfirmationsListener)
      initialPublishChannel <- channelBuilder
      publishChannelRef <- Ref.of[F, (Channel[F], F[Unit])](initialPublishChannel)
      publishChannelManager = PublishChannelManager(channelBuilder, publishChannelRef, pendingConfirmationsListener)
      _ <- initialPublishChannel._1.addShutdownListener(publishChannelManager)
    }
      yield publishChannelManager
}
