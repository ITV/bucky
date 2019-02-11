package com.itv.bucky.fs2
import cats.effect.{IO, Sync, Timer}
import com.itv.bucky._
import com.itv.bucky.decl.{Exchange, Topic}
import com.itv.bucky.pattern.requeue.requeueDeclarations
import _root_.fs2.Stream
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext

package object examples {
  trait App {
    def amqp: Stream[IO, Unit]
  }

  object App {

    import UnmarshalResultOps._
    import scala.concurrent.duration._

    trait Bar {
      def add(message: String): IO[Unit]
    }

    def apply(amqpClient: IOAmqpClient,
              bar: Bar)(implicit executionContext: ExecutionContext, timer: Timer[IO], F: Sync[IO]) =
      new App with StrictLogging {
        override def amqp = amqpClient.consumer(
          RmqConfig.Source.queueName,
          Handler[IO, Delivery] {
            _.body.unmarshal[String].success match {
              case s if s.startsWith("bar") => bar.add(s).map(_ => Ack)
              case s if s.startsWith("no_end") =>
                Stream
                  .awakeEvery[IO](10.millis)
                  .evalMap(_ =>
                    IO {
                      logger.debug(s"$s")
                  })
                  .compile
                  .last
                  .map(_ => Ack)
              case other =>
                amqpClient
                  .publisher()(RmqConfig.Target.stringPublishCommandBuilder.toPublishCommand(other))
                  .map(_ => Ack)
            }
          }
        )
      }

    object RmqConfig {
      object NoConsumer {
        val exchangeName = ExchangeName("no_consumer")
        val routingKey   = RoutingKey("go.no_consumer")
        val queueName    = QueueName("q.no_consumer")
        val declaration = Exchange(exchangeName, exchangeType = Topic)
          .binding(routingKey -> queueName)
        val requeueDeclaration = requeueDeclarations(queueName, routingKey)

        val stringPublishCommandBuilder = RabbitSimulator.stringPublishCommandBuilder using exchangeName using routingKey
      }

      object InvalidBinding {

        val stringPublishCommandBuilder = RabbitSimulator.stringPublishCommandBuilder using ExchangeName("no_binding") using RoutingKey(
          "no_binding")
      }

      object Source {
        val exchangeName = ExchangeName("source")
        val routingKey   = RoutingKey("go.to.source")
        val queueName    = QueueName("q.go.to.source")
        val declaration = Exchange(exchangeName, exchangeType = Topic)
          .binding(routingKey -> queueName)
        val requeueDeclaration = requeueDeclarations(queueName, routingKey)

        val stringPublishCommandBuilder = RabbitSimulator.stringPublishCommandBuilder using exchangeName using routingKey
      }

      object Target {

        val exchangeName = ExchangeName("target")
        val routingKey   = RoutingKey("go.to.target")
        val queueName    = QueueName("q.go.to.target")

        val stringPublishCommandBuilder = RabbitSimulator.stringPublishCommandBuilder using exchangeName using routingKey

      }

      val all = Source.requeueDeclaration ++ List(Source.declaration) ++ NoConsumer.requeueDeclaration ++ List(
        NoConsumer.declaration)
    }

  }
}
