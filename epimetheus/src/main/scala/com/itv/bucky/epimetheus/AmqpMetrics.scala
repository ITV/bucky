package com.itv.bucky.epimetheus

import cats.implicits._
import com.itv.bucky.AmqpClient
import io.chrisdavenport.epimetheus.CollectorRegistry
import scala.language.higherKinds
import com.rabbitmq.client.MetricsCollector
import cats.effect.kernel.Sync
import io.chrisdavenport.epimetheus.Name
import io.chrisdavenport.epimetheus.Gauge
import io.chrisdavenport.epimetheus._
import com.itv.bucky.decl.Declaration
import com.itv.bucky.Publisher
import com.itv.bucky.publish.PublishCommand
import cats.effect.Resource
import com.itv.bucky.consume.{ConsumeAction, Delivery}
import com.itv.bucky.{Handler, QueueName}
import scala.concurrent.duration.FiniteDuration

object AmqpMetrics {
  private case class MetricsCollection[F[_]](
      connections: Gauge[F],
      channels: Gauge[F],
      publishedMessages: Counter[F],
      consumedMessages: Counter[F],
      ackowledgedMessages: Counter[F],
      rejectedMessages: Counter[F],
      failedToPublishMessages: Counter[F],
      ackowledgedPublishedMessages: Counter[F],
      notAckowledgedPublishedMessages: Counter[F],
      unroutedPublishedMessages: Counter[F]
  )

  private object MetricsCollection {
    def make[F[_]: Sync](
        cr: CollectorRegistry[F],
        prefix: Name
    ): F[MetricsCollection[F]] =
      for {
        connections <- Gauge.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("connections"),
          "Number of current active connections"
        )
        channels <- Gauge.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("channels"),
          "Number of current active connections"
        )
        publishedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("published"),
          "TODO"
        )
        consumedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("consumed"),
          "TODO"
        )

        acknowledgedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("acknowledged"),
          "TODO"
        )
        rejectedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("rejected"),
          "TODO"
        )
        failedToPublishMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("failed_to_publish"),
          "TODO"
        )
        ackowledgedPublishedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("acknowledged_published"),
          "TODO"
        )
        notAckowledgedPublishedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("not_acknowledged_published"),
          "TODO"
        )
        unroutedPublishedMessages <- Counter.noLabels(
          cr,
          prefix ++ Name("_") ++ Name("consumed_messages"),
          "TODO"
        )
      } yield MetricsCollection[F](
        connections,
        channels,
        publishedMessages,
        consumedMessages,
        acknowledgedMessages,
        rejectedMessages,
        failedToPublishMessages,
        ackowledgedPublishedMessages,
        notAckowledgedPublishedMessages,
        unroutedPublishedMessages
      )

  }

  def apply[F[_]: Sync](
      amqpClient: AmqpClient[F],
      collectorRegistry: CollectorRegistry[F]
  ): F[AmqpClient[F]] = MetricsCollection.make[F](collectorRegistry, Name("bucky")).map { metrics =>
    new AmqpClient[F] {

      def declare(declarations: Declaration*): F[Unit] = ???

      def declare(declarations: Iterable[Declaration]): F[Unit] = ???

      def publisher(): Publisher[F, PublishCommand] = {
        val unmetred = amqpClient.publisher()

        cmd => unmetred(cmd) <* metrics.publishedMessages.inc
      }

      def registerConsumer(
          queueName: QueueName,
          handler: Handler[F, Delivery],
          exceptionalAction: ConsumeAction,
          prefetchCount: Int,
          shutdownTimeout: FiniteDuration,
          shutdownRetry: FiniteDuration
      ): Resource[F, Unit] = ???

      def isConnectionOpen: F[Boolean] = ???
    }

  }
}
