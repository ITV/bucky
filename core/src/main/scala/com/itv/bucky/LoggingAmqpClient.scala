package com.itv.bucky

import cats.effect.{ConcurrentEffect}
import com.typesafe.scalalogging.StrictLogging
import cats._
import cats.implicits._
import java.nio.charset.Charset

import com.itv.bucky.consume.{ConsumeAction, Delivery, PublishCommand}

import scala.language.higherKinds

object LoggingAmqpClient extends StrictLogging {
  def apply[F[_]](amqpClient: AmqpClient[F], charset: Charset)(implicit F: ConcurrentEffect[F]): AmqpClient[F] =
    new AmqpClient[F] {
      override def declare(declarations: decl.Declaration*): F[Unit]          = amqpClient.declare(declarations)
      override def declare(declarations: Iterable[decl.Declaration]): F[Unit] = amqpClient.declare(declarations)

      override def publisher(): Publisher[F, PublishCommand] = {
        val originalPublisher = amqpClient.publisher()
        cmd: PublishCommand =>
          {
            (for {
              result <- originalPublisher(cmd).attempt
              _ <- F.delay(
                result
                  .map(_ => {
                    logger.info("Successfully published message with rk:'{}', exchange:{} and message:'{}'",
                                cmd.routingKey.value,
                                cmd.exchange.value,
                                new String(cmd.body.value, charset))
                  })
                  .leftMap(t => {
                    logger.error("Failed to publish message with rk:'{}', exchange:'{}' and message:'{}'",
                                 cmd.routingKey.value,
                                 cmd.exchange.value,
                                 new String(cmd.body.value, charset),
                                 t)
                  })
              )
            } yield result).rethrow
          }
      }

      override def registerConsumer(queueName: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction): F[Unit] = {
        val newHandler = (delivery: Delivery) => {
          (for {
            result <- handler(delivery).attempt
            _ <- F.delay {
              result
                .map(ca =>
                  logger.info(
                    "Executed handler for message with rk:'{}' on queue:'{}' and exchange '{}'. Will return '{}'. message: '{}'",
                    delivery.envelope.routingKey.value,
                    queueName.value,
                    delivery.envelope.exchangeName,
                    ca.toString.toLowerCase,
                    new String(delivery.body.value, charset)
                ))
                .leftMap(t =>
                  logger.error(
                    s"Failed to execute handler for message with rk '{}' on queue '{}' and exchange '{}'. Will return '{}'. message: '{}'",
                    delivery.envelope.routingKey.value,
                    queueName.value,
                    delivery.envelope.exchangeName,
                    new String(delivery.body.value, charset),
                    t
                ))
            }
          } yield result).rethrow
        }
        amqpClient.registerConsumer(queueName, newHandler, exceptionalAction)
      }

    }
}
