package com.itv.bucky.test.stubs

import cats.effect.Sync
import com.itv.bucky.{Handler, RequeueHandler}
import com.itv.bucky.consume.{ConsumeAction, RequeueConsumeAction}

import scala.language.higherKinds
import cats.effect._
import cats.implicits._

import scala.collection.mutable.ListBuffer

case class ExecutionResult[T, CA](message: T, result: Either[Throwable, CA])

class RecordingHandler[F[_], T](handler: Handler[F, T])(implicit F: Sync[F]) extends Handler[F, T] {
  private val results: ListBuffer[ExecutionResult[T, ConsumeAction]]         = ListBuffer.empty
  def executions: List[ExecutionResult[T, ConsumeAction]]                    = results.synchronized(results.toList)
  def receivedMessages: List[T]                               = executions.map(_.message)
  def returnedResults: List[Either[Throwable, ConsumeAction]] = executions.map(_.result)
  override def apply(message: T): F[ConsumeAction] =
    (for {
      result <- handler(message).attempt
    } yield {
      results.synchronized(results += ExecutionResult(message, result))
      result
    }).rethrow
}

class RecordingRequeueHandler[F[_], T](handler: RequeueHandler[F, T])(implicit F: Sync[F]) extends RequeueHandler[F, T] {
  private val results: ListBuffer[ExecutionResult[T, RequeueConsumeAction]]         = ListBuffer.empty
  def executions: List[ExecutionResult[T, RequeueConsumeAction]]                    = results.synchronized(results.toList)
  def receivedMessages: List[T]                               = executions.map(_.message)
  def returnedResults: List[Either[Throwable, RequeueConsumeAction]] = executions.map(_.result)
  override def apply(message: T): F[RequeueConsumeAction] =
    (for {
      result <- handler(message).attempt
    } yield {
      results.synchronized(results += ExecutionResult(message, result))
      result
    }).rethrow
}