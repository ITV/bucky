package com.itv.bucky

import scala.concurrent.duration._

/**
  * AmqpClient configuration. Acts as a lifecycle factory for AmqpClient.
  */
case class AmqpClientConfig(host: String,
                            port: Int,
                            username: String,
                            password: String,
                            networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds),
                            networkRecoveryIntervalOnStart: Option[NetworkRecoveryOnStart] = Some(
                              NetworkRecoveryOnStart()),
                            virtualHost: Option[String] = None)

case class NetworkRecoveryOnStart(interval: Duration = 30.seconds, max: Duration = 15.minutes) {
  val numberOfRetries = max.toMillis / interval.toMillis
}
