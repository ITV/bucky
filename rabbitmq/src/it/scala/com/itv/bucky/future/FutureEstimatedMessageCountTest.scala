package com.itv.bucky.future

import com.itv.bucky.EstimatedMessageCountTest
import org.scalatest.FunSuite
import scala.concurrent.Future

import FutureExt._

class FutureEstimatedMessageCountTest extends FunSuite with EstimatedMessageCountTest[Future] with FuturePublisherTest {

  override def runAll(list: Seq[Future[Unit]]): Unit = Future.sequence(list)
}


