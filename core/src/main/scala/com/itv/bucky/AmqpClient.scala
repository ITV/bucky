package com.itv.bucky

import cats.effect._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery}
import com.itv.bucky.decl._
import com.itv.bucky.publish.PublishCommand

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

trait AmqpClient[F[_]] {
  def declare(declarations: Declaration*): F[Unit]
  def declare(declarations: Iterable[Declaration]): F[Unit]
  def publisher(): Publisher[F, PublishCommand]
  def registerConsumer(queueName: QueueName,
                       handler: Handler[F, Delivery],
                       exceptionalAction: ConsumeAction = DeadLetter,
                       prefetchCount: Int = 1,
                       shutdownTimeout: FiniteDuration = 1.minutes,
                       shutdownRetry: FiniteDuration = 500.millis): Resource[F, Unit]
  def isConnectionOpen: F[Boolean]
}
