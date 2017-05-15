package com.itv.bucky

import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FutureEstimatedMessageCountTest extends FunSuite with EstimatedMessageCountTest[Future] with FuturePublisherTest {
  override def runAll(list: Seq[Future[Unit]]): Unit = Future.sequence(list)
}


