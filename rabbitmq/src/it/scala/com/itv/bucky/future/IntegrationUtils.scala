package com.itv.bucky.future

import com.itv.bucky.decl._
import com.itv.bucky.{AmqpClientConfig, QueueName}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object IntegrationUtils {
  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

  def config: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    val host = config.getString("rmq.host")

    AmqpClientConfig(config.getString("rmq.host"), config.getInt("rmq.port"), config.getString("rmq.username"), config.getString("rmq.password"))
  }

}
