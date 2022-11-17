package com.itv.bucky.test.stubs

import cats.effect.Sync
import com.itv.bucky.{Handler, RequeueHandler}
import com.itv.bucky.consume.{ConsumeAction, RequeueConsumeAction}

import scala.language.higherKinds
import cats.effect._
import cats.implicits._
import com.itv.bucky.test.stubs.RecordingHandler.{ConsumeActionBufferRef, ListBufferRef, RequeueConsumeActionBufferRef}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer

case class ExecutionResult[T, CA](message: T, result: Either[Throwable, CA])

object RecordingHandler {
  type ListBufferRef[F[_], T, A] = Ref[F, ListBuffer[ExecutionResult[T, A]]]

  type ConsumeActionBufferRef[F[_], T] = Ref[F, ListBuffer[ExecutionResult[T, ConsumeAction]]]
  type RequeueConsumeActionBufferRef[F[_], T] = Ref[F, ListBuffer[ExecutionResult[T, RequeueConsumeAction]]]

  def createRef[F[_] : Sync,T,A] =
    Ref[F].of(ListBuffer[ExecutionResult[T, A]]())

  private def createRefResource[F[_] : Sync, T, A](): Resource[F, ListBufferRef[F, T, A]] =
    Resource.eval(Ref[F].of(ListBuffer[ExecutionResult[T, A]]()))

  def createConsumeActionBufferRef[F[_] : Sync, T]() =
    createRefResource[F, T, ConsumeAction]()

  def createRequeueConsumeActionBufferRef[F[_]: Sync, T] =
    createRefResource[F, T, RequeueConsumeAction]()
}

class RecordingHandler[F[_], T](handler: Handler[F, T])(implicit F: Sync[F]) extends Handler[F, T] {

  val buffer = new AtomicReference[List[ExecutionResult[T, ConsumeAction]]](List.empty)

  def executions: List[ExecutionResult[T, ConsumeAction]]  = buffer.get()
  def receivedMessages: List[T]                               = executions.map(_.message)
  def returnedResults: List[Either[Throwable, ConsumeAction]] = executions.map(_.result)

  override def apply(message: T): F[ConsumeAction] =
    (for {
      result <- handler(message).attempt
      _ <- F.blocking(buffer.updateAndGet(buffer => buffer :+ ExecutionResult(message, result)))
    } yield {
      result
    }).rethrow
}

class RecordingRequeueHandler[F[_], T](handler: RequeueHandler[F, T])(implicit F: Sync[F]) extends RequeueHandler[F, T] {

  val buffer = new AtomicReference[List[ExecutionResult[T, RequeueConsumeAction]]](List.empty)

  def executions: List[ExecutionResult[T, RequeueConsumeAction]] = buffer.get()

  def receivedMessages: List[T] = executions.map(_.message)

  def returnedResults: List[Either[Throwable, RequeueConsumeAction]] = executions.map(_.result)

  override def apply(message: T): F[RequeueConsumeAction] =
    (for {
      result <- handler(message).attempt
      _ <- F.blocking(buffer.updateAndGet(buffer => buffer :+ ExecutionResult(message, result)))
    } yield {
      result
    }).rethrow
}