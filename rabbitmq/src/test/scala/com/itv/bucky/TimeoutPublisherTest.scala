package com.itv.bucky

import java.util.concurrent.{ScheduledExecutorService, TimeoutException}

import com.itv.lifecycle.{ExecutorLifecycles, Lifecycle}

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

class TimeoutPublisherTest extends FunSuite with ScalaFutures {
  import TestUtils._

  test("Returns result of delegate publisher if result occurs before timeout") {
    val command1 = anyPublishCommand()
    val command2 = anyPublishCommand()
    val expectedOutcome1 = Success(())
    val expectedOutcome2 = Failure(new RuntimeException("Bang!"))
    val delegate: Publisher[PublishCommand] = {
      case `command1` => Future.fromTry(expectedOutcome1)
      case `command2` => Future.fromTry(expectedOutcome2)
      case PublishCommand(_, _, _, _) => fail("Unexpected outcome")
    }
    val service = mock[ScheduledExecutorService]
    when(service.execute(any[Runnable]())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = invocation.getArguments()(0).asInstanceOf[Runnable].run()
    })

    val publisher = new TimeoutPublisher(delegate, 5.seconds)(service)

    publisher(command1).asTry.futureValue shouldBe expectedOutcome1
    publisher(command2).asTry.futureValue shouldBe expectedOutcome2
  }

  test("Returns timeout of delegate publisher if result occurs after timeout") {
    Lifecycle.using(ExecutorLifecycles.singleThreadScheduledExecutor) { scheduledExecutor =>
      val delegate: Publisher[PublishCommand] = { _ => Promise[Nothing]().future }

      val publisher = new TimeoutPublisher(delegate, 250.millis)(scheduledExecutor)

      val future = publisher(anyPublishCommand()).asTry

      future.value shouldBe None

      val result = Await.result(future, 500.millis)
      result shouldBe 'failure
      result.failed.get shouldBe a[TimeoutException]
      result.failed.get.getMessage should include("Timed out").and(include("250 millis"))
    }
  }
}
