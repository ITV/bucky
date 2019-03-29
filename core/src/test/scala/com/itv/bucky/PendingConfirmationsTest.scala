package com.itv.bucky

import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.{Deferred, Ref}
import org.scalatest.FunSuite
import cats.implicits._
import cats.effect.implicits._

import scala.collection.immutable.TreeMap

import org.scalatest.Matchers._

class PendingConfirmationsTest extends FunSuite {

  test("complete with true after confirmation ack") {
    implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)
    (for {
      signal <- Deferred[IO, Boolean]
      map = TreeMap(1L -> signal)
      ref <- Ref.of[IO, TreeMap[Long, Deferred[IO, Boolean]]](map)
      pendingConfirmListener = PendingConfirmListener(ref)
      _                 <- IO.delay(pendingConfirmListener.handleAck(1L, multiple = false))
      refAfterUpdate    <- ref.get
      signalAfterUpdate <- signal.get
    } yield {
      signalAfterUpdate shouldBe true
      refAfterUpdate shouldBe 'empty
    }).unsafeRunSync()
  }

  test("complete with true after confirmation ack (multiple)") {
    implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)
    (for {
      signal1 <- Deferred[IO, Boolean]
      signal2 <- Deferred[IO, Boolean]
      signal3 <- Deferred.tryable[IO, Boolean]

      map = TreeMap(1L -> signal1, 2L -> signal2, 3L -> signal3)
      ref <- Ref.of[IO, TreeMap[Long, Deferred[IO, Boolean]]](map)

      pendingConfirmListener = PendingConfirmListener(ref)
      _ <- IO.delay(pendingConfirmListener.handleAck(2L, multiple = true))

      refAfterUpdate     <- ref.get
      signal1AfterUpdate <- signal1.get
      signal2AfterUpdate <- signal2.get
      signal3AfterUpdate <- signal3.tryGet
    } yield {
      refAfterUpdate should have size 1
      refAfterUpdate(3L) shouldBe signal3

      signal1AfterUpdate shouldBe true
      signal2AfterUpdate shouldBe true
      signal3AfterUpdate shouldBe None
    }).unsafeRunSync()
  }

  test("should remove pending confirmations after a publish timeout") {
    implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)
    (for {
      signal <- Deferred.tryable[IO, Boolean]
      map = TreeMap(1L -> signal)
      ref <- Ref.of[IO, TreeMap[Long, Deferred[IO, Boolean]]](map)
      pendingConfirmListener = PendingConfirmListener(ref)
      _                 <- IO.delay(pendingConfirmListener.handleAck(1L, multiple = false))
      refAfterUpdate    <- ref.get.attempt
      signalAfterUpdate <- signal.tryGet
    } yield {
      signalAfterUpdate shouldBe None
      refAfterUpdate shouldBe 'empty
    }).unsafeRunSync()
  }

}
