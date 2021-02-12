package com.itv.bucky

import cats.effect.{ConcurrentEffect, Resource}
import com.itv.bucky.consume._
import com.itv.bucky.publish._
import org.typelevel.log4cats.Logger

import java.nio.charset.Charset
import scala.language.higherKinds
import cats.syntax.all._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object LoggingAmqpClient {

  private[bucky] def logSuccessfullPublishMessage[F[_]](charset: Charset, cmd: PublishCommand)(implicit F: ConcurrentEffect[F],
                                                                                               logger: Logger[F]): F[Unit] =
    logger.info(
      s"Successfully published message with rk:${cmd.routingKey.value}, exchange:${cmd.exchange.value} and message:${new String(cmd.body.value, charset)}")

  private[bucky] def logFailedPublishMessage[F[_]](t: Throwable, charset: Charset, cmd: PublishCommand)(implicit F: ConcurrentEffect[F],
                                                                                                        logger: Logger[F]): F[Unit] =
    logger.error(t)(
      s"Failed to publish message with rk:${cmd.routingKey.value}, exchange:${cmd.exchange.value} and message:${new String(cmd.body.value, charset)}")

  private[bucky] def logFailedHandler[F[_]](charset: Charset,
                                            queueName: QueueName,
                                            exceptionalAction: ConsumeAction,
                                            delivery: Delivery,
                                            t: Throwable)(implicit F: ConcurrentEffect[F], logger: Logger[F]): F[Unit] =
    logger.error(t)(
      s"Failed to execute handler for message with rk ${delivery.envelope.routingKey.value} on queue ${queueName.value} and exchange ${delivery.envelope.exchangeName}. Will return $exceptionalAction. message: ${new String(
        delivery.body.value,
        charset)}, headers:${delivery.properties.headers}")

  private[bucky] def logSuccessfulHandler[F[_]](charset: Charset, queueName: QueueName, delivery: Delivery, ca: ConsumeAction)(
      implicit F: ConcurrentEffect[F],
      logger: Logger[F]): F[Unit] =
    logger.info(
      s"Executed handler for message with rk:${delivery.envelope.routingKey.value} on queue:${queueName.value} and exchange ${delivery.envelope.exchangeName}. Will return ${ca.toString.toLowerCase}. message: ${new String(delivery.body.value, charset)}")

  def apply[F[_]](amqpClient: AmqpClient[F], charset: Charset)(implicit F: ConcurrentEffect[F], logger: Logger[F]): AmqpClient[F] =
    new AmqpClient[F] {
      override def declare(declarations: decl.Declaration*): F[Unit]          = amqpClient.declare(declarations)
      override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = amqpClient.declare(declarations)

      override def publisher(): Publisher[F, PublishCommand] = {
        val originalPublisher = amqpClient.publisher()
        (cmd: PublishCommand) =>
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
