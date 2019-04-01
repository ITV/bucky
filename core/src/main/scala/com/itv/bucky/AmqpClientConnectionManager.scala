package com.itv.bucky

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import com.itv.bucky.decl.Declaration
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.TreeMap
import scala.language.higherKinds
import scala.util.Try

private[bucky] case class AmqpClientConnectionManager[F[_]](
    amqpConfig: AmqpClientConfig,
    channel: Channel[F],
    pendingConfirmListener: PendingConfirmListener[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F])
    extends StrictLogging {

  private def runWithChannelSync[T](action: F[T]): F[T] =
    channel.synchronized {
      F.fromTry(Try {
        action.toIO.unsafeRunSync()
      })
    }

  def publish(cmd: PublishCommand): F[Unit] =
    for {
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, Boolean]
        _ <- runWithChannelSync {
          for {
            nextPublishSeq <- channel.getNextPublishSeqNo
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfirmListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- channel.publish(cmd)
          } yield ()
        }
        _ <- signal.get.ifM(F.unit, F.raiseError[Unit](new RuntimeException("Failed to publish msg.")))
      } yield ())
        .timeout(amqpConfig.publishingTimeout)
        .recoverWith {
          case e =>
            runWithChannelSync {
              for {
                dl          <- deliveryTag.get
                deliveryTag <- F.fromOption(dl, new RuntimeException("Timeout occurred before a delivery tag could be obtained.", e))
                _           <- pendingConfirmListener.pop(deliveryTag, multiple = false)
                _           <- F.raiseError[Unit](e)
              } yield ()
            }
        }
    } yield ()

  def registerConsumer(queueName: QueueName, handler: Handler[F, Delivery], onFailure: ConsumeAction, prefetchCount: Int): F[Unit] =
    for {
      _           <- cs.shift
      consumerTag <- F.delay(ConsumerTag.create(queueName))
      _           <- F.delay(logger.debug("Registering consumer for queue: {} with tag {}.", queueName.value, consumerTag.value))
      _           <- channel.basicQos(prefetchCount)
      _           <- channel.registerConsumer(handler, onFailure, queueName, consumerTag)
      _           <- F.delay(logger.debug("Consumer for queue: {} with tag {} was successfully registered.", queueName.value, consumerTag.value))
      _           <- F.delay(logger.debug("Successfully registered consumer for queue: {} with tag.", queueName.value), consumerTag.value)
    } yield ()
  def shutdown(): F[Unit]                                   = channel.shutdownChannelAndConnection()
  def declare(declarations: Iterable[Declaration]): F[Unit] = channel.runDeclarations(declarations)
}

private[bucky] object AmqpClientConnectionManager extends StrictLogging {

  def apply[F[_]](config: AmqpClientConfig,
                  channel: Channel[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F]): F[AmqpClientConnectionManager[F]] =
    for {
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Boolean]]](TreeMap.empty)
      _                    <- channel.confirmSelect
      confirmListener      <- F.delay(PendingConfirmListener(pendingConfirmations))
      _                    <- channel.addConfirmListener(confirmListener)
    } yield AmqpClientConnectionManager(config, channel, confirmListener)
}
