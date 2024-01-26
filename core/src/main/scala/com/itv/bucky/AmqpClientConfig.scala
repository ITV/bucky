package com.itv.bucky

import scala.concurrent.duration._

/** AmqpClient configuration.
  */
case class AmqpClientConfig(
    host: String,
    port: Int,
    username: String,
    password: String,
    networkRecoveryInterval: Option[FiniteDuration] = Some(3.seconds),
    networkRecoveryIntervalOnStart: Option[NetworkRecoveryOnStart] = Some(NetworkRecoveryOnStart()),
    publishingTimeout: FiniteDuration = 15.seconds,
    virtualHost: Option[String] = None,
    connectionTimeout: FiniteDuration = 10.seconds,
    ssl: Boolean = false,
    requeueOnNack: Boolean = false,
    requeueOnReject: Boolean = true,
    internalQueueSize: Option[Int] = None
)

case class NetworkRecoveryOnStart(interval: FiniteDuration = 3.seconds, max: FiniteDuration = 3.seconds) {
  val numberOfRetries: Long = max.toMillis / interval.toMillis
}
