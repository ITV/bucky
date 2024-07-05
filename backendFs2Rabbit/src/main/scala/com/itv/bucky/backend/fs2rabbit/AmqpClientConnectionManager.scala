package com.itv.bucky.backend.fs2rabbit

import cats.effect._
import cats.effect.implicits._
import cats.effect.std.Dispatcher
import cats.implicits._
import com.itv.bucky.publish._
import com.itv.bucky.{AmqpClientConfig, Publisher, publish}
import com.rabbitmq.client.Channel
import com.typesafe.scalalogging.StrictLogging
import dev.profunktor.fs2rabbit.effects.MessageEncoder
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model
import dev.profunktor.fs2rabbit.model.{AMQPChannel, PublishingFlag}

import scala.collection.immutable.TreeMap
import scala.util.Try

class AmqpClientConnectionManager[F[_]: Async](
    amqpConfig: AmqpClientConfig,
    pendingConfirmListener: PendingConfirmListener[F],
    dispatcher: Dispatcher[F],
    client: RabbitClient[F]
)(implicit publishCommandEncoder: MessageEncoder[F, PublishCommand], amqpChannel: AMQPChannel)
    extends StrictLogging {

  val publishChannel: Channel = amqpChannel.value

  private def synchroniseIfNeeded[T](f: => T): T = publishChannel.synchronized(f)
  private def runWithChannelSync[T](action: F[T]): F[T] =
    synchroniseIfNeeded {
      Async[F].fromTry(Try {
        dispatcher.unsafeRunSync(action)
      })
    }

  def addConfirmListeningToPublisher(publisher: Publisher[F, PublishCommand]): Publisher[F, PublishCommand] = (cmd: PublishCommand) =>
    for {
      deliveryTag <- Ref.of[F, Option[Long]](None)
      _ <- (for {
        signal <- Deferred[F, Option[Throwable]]
        _ <- runWithChannelSync {
          for {
            nextPublishSeq <- Async[F].blocking(publishChannel.getNextPublishSeqNo)
            _              <- deliveryTag.set(Some(nextPublishSeq))
            _              <- pendingConfirmListener.pendingConfirmations.update(_ + (nextPublishSeq -> signal))
            _              <- publisher(cmd)
          } yield ()
        }
        _ <- signal.get.flatMap(maybeError => maybeError.traverse(Async[F].raiseError[Unit]))
      } yield ())
        .timeout(amqpConfig.publishingTimeout)
        .recoverWith { case e =>
          runWithChannelSync {
            for {
              dl          <- deliveryTag.get
              deliveryTag <- Async[F].fromOption(dl, new RuntimeException("Timeout occurred before a delivery tag could be obtained.", e))
              _           <- pendingConfirmListener.pop(deliveryTag, multiple = false)
              _           <- Async[F].raiseError[Unit](e)
            } yield ()
          }
        }
    } yield ()

}

private[bucky] object AmqpClientConnectionManager extends StrictLogging {

  def apply[F[_]](
      config: AmqpClientConfig,
      client: RabbitClient[F],
      amqpChannel: AMQPChannel,
      dispatcher: Dispatcher[F]
  )(implicit
      F: Async[F],
      publishCommandEncoder: MessageEncoder[F, PublishCommand]
  ): F[AmqpClientConnectionManager[F]] =
    for {
      pendingConfirmations <- Ref.of[F, TreeMap[Long, Deferred[F, Option[Throwable]]]](TreeMap.empty)
      pendingReturn        <- Ref.of[F, Option[Throwable]](None)
      publishChannel = amqpChannel.value
      _               <- Async[F].blocking(publishChannel.confirmSelect)
      confirmListener <- Async[F].blocking(PendingConfirmListener(pendingConfirmations, pendingReturn, dispatcher))
      _               <- Async[F].blocking(publishChannel.addConfirmListener(confirmListener))
      _               <- Async[F].blocking(publishChannel.addReturnListener(confirmListener))
    } yield new AmqpClientConnectionManager(config, confirmListener, dispatcher, client)(implicitly, implicitly, amqpChannel)
}
