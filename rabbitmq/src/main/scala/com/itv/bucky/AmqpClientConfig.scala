package com.itv.bucky

import scala.concurrent.duration._

/**
  * AmqpClient configuration. Acts as a lifecycle factory for AmqpClient.
  */
case class AmqpClientConfig(
                             host: String,
                             port: Int,
                             username: String,
                             password: String,
                             networkRecoveryInterval: Option[FiniteDuration] = Some(5.seconds),
                             virtualHost: Option[String] = None)
