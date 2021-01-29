package com.itv.bucky

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import com.itv.bucky.decl.Declaration
import com.itv.bucky.publish.PendingConfirmListener
import org.typelevel.log4cats.Logger

import scala.collection.immutable.TreeMap
import scala.language.higherKinds
import scala.util.Try

private[bucky] case class AmqpClientConnectionManager[F[_]](
                                                             amqpConfig: AmqpClientConfig,
                                                             publishChannel: Channel[F],
                                                             pendingConfirmListener: PendingConfirmListener[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F], logger: Logger[F]) {

  private def runWithChannelSync[T](action: F[T]): F[T] =
    publishChannel.synchroniseIfNeeded {
      F.fromTry(Try {
        action.toIO.unsafeRunSync()
      })
    }

  def publish(cmd: PublishCommand): F[Unit] =
    for {
      _ <- cs.shift
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, Boolean]
        _ <- runWithChannelSync {
          for {
            nextPublishSeq <- publishChannel.getNextPublishSeqNo
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfirmListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- publishChannel.publish(nextPublishSeq, cmd)
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

  def registerConsumer(channel: Channel[F], queueName: QueueName, handler: Handler[F, Delivery], onHandlerException: ConsumeAction, prefetchCount: Int): F[Unit] =
    for {
      _ <- cs.shift
      consumerTag <- F.delay(ConsumerTag.create(queueName))
      _ <- logger.debug(s"Registering consumer for queue: ${queueName.value} with tag ${consumerTag.value}.")
      _ <- channel.basicQos(prefetchCount)
      _ <- channel.registerConsumer(handler, onHandlerException, queueName, consumerTag, cs)
      _ <- logger.debug(s"Consumer for queue: ${queueName.value} with tag ${consumerTag.value} was successfully registered.")
      _ <- logger.debug(s"Successfully registered consumer for queue: ${queueName.value} with tag.")
    } yield ()

  def declare(declarations: Iterable[Declaration]): F[Unit] = publishChannel.runDeclarations(declarations)
}

private[bucky] object AmqpClientConnectionManager {

  def apply[F[_]](config: AmqpClientConfig,
                  publishChannel: Channel[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F], logger: Logger[F]): F[AmqpClientConnectionManager[F]] =
    for {
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Boolean]]](TreeMap.empty)
      _                    <- publishChannel.confirmSelect
      confirmListener      <- F.delay(publish.PendingConfirmListener(pendingConfirmations))
      _                    <- publishChannel.addConfirmListener(confirmListener)
    } yield AmqpClientConnectionManager(config, publishChannel, confirmListener)
}
