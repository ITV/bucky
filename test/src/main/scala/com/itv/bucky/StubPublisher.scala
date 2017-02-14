package com.itv.bucky

import itv.bucky.Publisher

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class StubPublisher[A] extends Publisher[A] {

  val publishedEvents = ListBuffer[A]()

  private var nextResponse: Future[Unit] = Future.successful(())

  def respondToNextPublishWith(expectedResult: Future[Unit]) = nextResponse = expectedResult

  override def apply(event: A): Future[Unit] = {
    publishedEvents += event
    nextResponse
  }
}