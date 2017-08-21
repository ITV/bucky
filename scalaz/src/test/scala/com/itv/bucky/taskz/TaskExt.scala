package com.itv.bucky.taskz
import java.util.concurrent.atomic.AtomicReference

import com.itv.bucky.AtomicRef.Ref
import com.itv.bucky.{PublishCommand, Publisher, StubChannel, TestFixture}
import org.scalactic.source

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{FiniteDuration, _}
import scalaz.{\/, \/-}
import scalaz.concurrent.Task

object TaskExt {

  implicit val executorService = ExecutionContextExecutorServiceBridge(new ExecutionContextExecutor {
    override def execute(runnable: Runnable): Unit = runnable.run()

    override def reportFailure(cause: Throwable): Unit = throw cause
  })

  implicit val taskMonad = taskMonadError(executorService)

  type TaskResult = \/[Throwable, Unit]

  def resultFrom(task: Task[Unit]): TaskStatus = {
    val status = TaskStatus(new AtomicReference[Option[TaskResult]](None))
    task.unsafePerformAsync { result =>
      status.complete(result)
    }
    status
  }

  case class TestPublisher(channel: StubChannel, publish: Publisher[Task, PublishCommand])

  def withPublisher(timeout: FiniteDuration = 1.second, channel: StubChannel = new StubChannel)(
      f: TestPublisher => Unit): Unit = {
    val client  = TaskAmqpClient(channel)
    val publish = client.publisher(timeout)
    f(TestPublisher(channel, publish))
  }

  case class TaskStatus(status: Ref[Option[TaskResult]]) {

    import org.scalatest.Matchers.fail

    def complete(result: \/[Throwable, Unit]) = status.set(Some(result))

    def isRunning = status.get().isEmpty

    def isCompleted = status.get().isDefined

    def isSuccess: Boolean = status.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Boolean](
        (e: Throwable) => fail(s"It should not fail"),
        _ => true
      )
    }

    def failure: Throwable = status.get().fold(fail(s"It is running!!!")) { result =>
      result.fold[Throwable](
        identity,
        _ => fail("It should not be completed successfully")
      )
    }
  }

}

object PublishExt {
  import org.scalatest.concurrent.Eventually._
  import org.scalatest.Matchers._
  val success = \/-(())

  def publish(app: TestFixture[Task], command: PublishCommand)(implicit config: PatienceConfig, pos: source.Position) =
    eventually {
      app.publisher(command).unsafePerformSyncAttempt should ===(success)
    }(config, pos)

}
