package com.itv.bucky

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.itv.bucky.PayloadMarshaller.StringPayloadMarshaller
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky.backend.fs2rabbit.Fs2RabbitAmqpClient
import com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient
import com.itv.bucky.decl.Exchange
import com.itv.bucky.pattern.requeue.RequeuePolicy
import com.itv.bucky.publish.{PublishCommand, PublishCommandBuilder}
import com.itv.bucky.{AmqpClient, AmqpClientConfig, ExchangeName, PayloadMarshaller, PayloadUnmarshaller, Publisher, RoutingKey, publishCommandBuilder}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class PublishIntegrationTest extends AnyFunSuite with IntegrationSpec with Eventually with IntegrationPatience {

  implicit val ioRuntime: IORuntime = packageIORuntime
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300))
  val requeuePolicy: RequeuePolicy = RequeuePolicy(maximumProcessAttempts = 5, requeueAfter = 2.seconds)

  test("publisher should error if mandatory is and there is no routing") {
    withTestFixture(mandatory = true){
      case (builder, publisher) =>
        publisher(builder.toPublishCommand("Where am I going?")).attempt.map(res => {
          println("result: " + res)
          res.isLeft shouldBe true
        }
        )
    }
  }
  test("publisher should publish if mandatory is false and there is no routing") {
    withTestFixture(mandatory = false) {
      case (builder, publisher) =>
        publisher(builder.toPublishCommand("But seriously though, where am I going?")).attempt.map(_.isRight shouldBe true)
    }

  }

  def withTestFixture(mandatory: Boolean)(test: (PublishCommandBuilder.Builder[String], Publisher[IO, PublishCommand]) => IO[Unit]): Unit = {
    val rawConfig = ConfigFactory.load("bucky")
    val config =
      AmqpClientConfig(rawConfig.getString("rmq.host"),
                       rawConfig.getInt("rmq.port"),
                       rawConfig.getString("rmq.username"),
                       rawConfig.getString("rmq.password"))
    implicit val payloadMarshaller: PayloadMarshaller[String]     = StringPayloadMarshaller
    implicit val payloadUnmarshaller: PayloadUnmarshaller[String] = StringPayloadUnmarshaller

    val exchangeName = ExchangeName(UUID.randomUUID().toString)
    val routingKey   = RoutingKey(UUID.randomUUID().toString)

    Fs2RabbitAmqpClient[IO](config)
      .use { client =>
        Resource
          .eval(
            client.declare(Exchange(exchangeName))
          )
          .use { _ =>
            val pcb = publishCommandBuilder[String](implicitly).using(exchangeName).using(routingKey)
            client.publisher(mandatory).flatMap { pub =>
              test(pcb, pub)
            }
          }
      }
      .unsafeRunSync()
  }

}
