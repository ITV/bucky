package com.itv.bucky.wiring
import java.util.concurrent.Executors

import com.itv.bucky.AmqpClientConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, Suite}

import scala.concurrent.ExecutionContext

trait WiringIntegrationTest
  extends Eventually
    with Matchers
    with ScalaFutures
    with StrictLogging
    with WiringTestOps { self: Suite =>

  implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(Span(10, Seconds), Span(100, Millis))

  def amqpConfig: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    AmqpClientConfig(config.getString("rmq.host"),
                     config.getInt("rmq.port"),
                     config.getString("rmq.username"),
                     config.getString("rmq.password"))
  }

}
