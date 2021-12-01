package com.itv.bucky

import cats.effect.{Resource, Sync}
import com.typesafe.scalalogging.StrictLogging
import cats._
import cats.implicits._

import java.nio.charset.Charset
import com.itv.bucky.consume._
import com.itv.bucky.publish._

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object LoggingAmqpClient extends StrictLogging {

  private[bucky] def logSuccessfullPublishMessage[F[_]](charset: Charset, cmd: PublishCommand)(implicit F: Sync[F]): F[Unit] =
    F.delay(
      logger.info("Successfully published message with rk:'{}', exchange:{} and message:'{}'",
                  cmd.routingKey.value,
                  cmd.exchange.value,
                  new String(cmd.body.value, charset))
    )

  private[bucky] def logFailedPublishMessage[F[_]](t: Throwable, charset: Charset, cmd: PublishCommand)(implicit F: Sync[F]): F[Unit] =
    F.delay(
      logger.error("Failed to publish message with rk:'{}', exchange:'{}' and message:'{}'",
                   cmd.routingKey.value,
                   cmd.exchange.value,
                   new String(cmd.body.value, charset),
                   t)
    )

  private[bucky] def logFailedHandler[F[_]](charset: Charset,
                                            queueName: QueueName,
                                            exceptionalAction: ConsumeAction,
                                            delivery: Delivery,
                                            t: Throwable)(implicit F: Sync[F]): F[Unit] = F.delay {
    logger.error(
      s"Failed to execute handler for message with rk '{}' on queue '{}' and exchange '{}'. Will return '{}'. message: '{}', headers:'{}'",
      delivery.envelope.routingKey.value,
      queueName.value,
      delivery.envelope.exchangeName,
      exceptionalAction,
      new String(delivery.body.value, charset),
      delivery.properties.headers,
      t
    )
  }

  private[bucky] def logSuccessfulHandler[F[_]](charset: Charset, queueName: QueueName, delivery: Delivery, ca: ConsumeAction)(
      implicit F: Sync[F]): F[Unit] = F.delay {
    logger.info(
      "Executed handler for message with rk:'{}' on queue:'{}' and exchange '{}'. Will return '{}'. message: '{}'",
      delivery.envelope.routingKey.value,
      queueName.value,
      delivery.envelope.exchangeName,
      ca.toString.toLowerCase,
      new String(delivery.body.value, charset)
    )
  }

  def apply[F[_]](amqpClient: AmqpClient[F], charset: Charset)(implicit F: Sync[F]): AmqpClient[F] =
    new AmqpClient[F] {
      override def declare(declarations: decl.Declaration*): F[Unit]          = amqpClient.declare(declarations)
      override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = amqpClient.declare(declarations)

      override def publisher(): Publisher[F, PublishCommand] = {
        val originalPublisher = amqpClient.publisher()
        cmd: PublishCommand =>
          {
            (for {
              result <- originalPublisher(cmd).attempt
              _      <- result.fold[F[Unit]](logFailedPublishMessage(_, charset, cmd), _ => logSuccessfullPublishMessage(charset, cmd))
            } yield result).rethrow
          }
      }

      override def registerConsumer(queueName: QueueName,
                                    handler: Handler[F, Delivery],
                                    exceptionalAction: ConsumeAction,
                                    prefetchCount: Int,
                                    shutdownTimeout: FiniteDuration = 1.minutes,
                                    shutdownRetry: FiniteDuration = 500.millis): Resource[F, Unit] = {
        val newHandler = (delivery: Delivery) => {
          (for {
            result <- handler(delivery).attempt
            _ <- result.fold(logFailedHandler(charset, queueName, exceptionalAction, delivery, _),
                             logSuccessfulHandler(charset, queueName, delivery, _))
          } yield result).rethrow
        }
        amqpClient.registerConsumer(queueName, newHandler, exceptionalAction, prefetchCount)
      }

      override def isConnectionOpen: F[Boolean] = amqpClient.isConnectionOpen
    }

}
