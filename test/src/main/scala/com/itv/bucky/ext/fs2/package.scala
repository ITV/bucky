package com.itv.bucky.ext

import _root_.fs2._
import cats.Show
import cats.implicits._
import cats.effect.{IO, Sync}
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.decl._

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.Assertion

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}
import scala.language.higherKinds

package object fs2 {

  trait MemoryAmqpSimulator[F[_]] extends AmqpClient[Id, IO, Throwable, Stream[IO, Unit]] {
    def publish(publishCommand: PublishCommand): F[async.Promise[F, ConsumeActionResult]]

    def publishAndWait(publishCommand: PublishCommand, timeout: FiniteDuration): F[ConsumeActionResult]

    def waitForMessagesToBeProcessed(timeout: FiniteDuration): F[List[ConsumeActionResult]]
  }
  object MemoryAmqpSimulator {
    case class Config(retryPolicy: RetryPolicy)
    object Config {
      import scala.concurrent.duration._
      val default = Config(RetryPolicy(10, 100.millis))
    }
    case class RetryPolicy(total: Int, sleep: FiniteDuration)

    object RetryPolicy {
      implicit val instanceShow: Show[RetryPolicy] = new Show[RetryPolicy] {
        override def show(t: RetryPolicy): String = s"after ${t.total} times with ${t.sleep} as sleep time"
      }
    }
  }

  sealed trait ConsumeActionResult

  object ConsumeActionResult {

    implicit val showInstances: Show[ConsumeActionResult] = new Show[ConsumeActionResult] {
      override def show(t: ConsumeActionResult): String = t match {
        case Consumed(value)               => value.toString
        case UnableToBeConsumed(_, reason) => reason
        case TimeoutToPublish(_, after)    => s"Timeout after $after"

      }
    }

    protected[fs2] case class Consumed(value: ConsumeAction) extends ConsumeActionResult
    protected[fs2] case class UnableToBeConsumed(publishCommand: PublishCommand, reason: String)
        extends ConsumeActionResult
    protected[fs2] case class TimeoutToPublish(publishCommand: PublishCommand, after: FiniteDuration)
        extends ConsumeActionResult

  }

  import com.itv.bucky.ext.fs2.ConsumeActionResult._
  implicit class ConsumeActionExt(value: ConsumeAction) {
    def result: ConsumeActionResult = Consumed(value)
  }

  implicit class PublishCommandConsumeActionExt(publishCommand: PublishCommand) {
    def timeout(timeout: FiniteDuration): ConsumeActionResult = TimeoutToPublish(publishCommand, timeout)

    def notBindingFound(retryPolicy: MemoryAmqpSimulator.RetryPolicy): ConsumeActionResult =
      UnableToBeConsumed(publishCommand, s"Not binding found ${retryPolicy.show}")
    def notConsumerFound(retryPolicy: MemoryAmqpSimulator.RetryPolicy): ConsumeActionResult =
      UnableToBeConsumed(publishCommand, s"Not consumer found ${retryPolicy.show}")
  }

  sealed trait Message

  protected[fs2] object Message {

    case class Source(publishCommand: PublishCommand, promise: async.Promise[IO, ConsumeActionResult]) extends Message
    case class Retry(source: Source, retries: Int, issue: Issue)                                       extends Message

    def sourceFrom(message: Message): Message.Source = message match {
      case Retry(source, _, _)   => source
      case source @ Source(_, _) => source
    }

    def retries(message: Message, expectedIssue: Issue): Int = message match {
      case Retry(_, retries, actualIssue) if actualIssue == expectedIssue => retries
      case _                                                              => 0
    }

    implicit val publishCommandShowInstance = new Show[PublishCommand] {
      override def show(publishCommand: PublishCommand): String =
        s"[${publishCommand.body.toString}] {${publishCommand.exchange} -> ${publishCommand.routingKey}}"
    }

    implicit val showInstance: Show[Message] = new Show[Message] {
      override def show(m: Message): String = m match {
        case Message.Source(publishCommand, _) =>
          publishCommand.show
        case Message.Retry(Message.Source(publishCommand, _), _, _) => publishCommand.show
      }
    }

    sealed trait Issue
    object Issue {
      case object NoBindingFound  extends Issue
      case object NoConsumerFound extends Issue
    }

    def noBindingFound(message: Message): Message.Retry = Message.Retry(
      sourceFrom(message),
      retries(message, Issue.NoBindingFound) + 1,
      Issue.NoBindingFound
    )

    def noConsumerFound(message: Message): Message.Retry = Message.Retry(
      sourceFrom(message),
      retries(message, Issue.NoConsumerFound) + 1,
      Issue.NoConsumerFound
    )

  }

  type Fs2AmqpSimulator = AmqpSimulator[Id, IO, Throwable, Stream[IO, Unit]]

  def rabbitSimulator(implicit executionContext: ExecutionContext,
                      ioMonadError: MonadError[IO, Throwable],
                      futureMonad: MonadError[Future, Throwable]): Fs2AmqpSimulator =
    old.rabbitSimulator

  def memorySimulator(config: MemoryAmqpSimulator.Config = MemoryAmqpSimulator.Config.default)(
      implicit executionContext: ExecutionContext,
      scheduler: Scheduler,
      idMonad: Monad[Id],
      ioMonadError: MonadError[IO, Throwable],
      F: Sync[IO]): IO[MemoryAmqpSimulator[IO]] =
    io.apply(config)

  implicit class RabbitSimulatorExt(amqpClient: AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]) extends StrictLogging {

    def consume(exchangeName: ExchangeName,
                routingKey: RoutingKey,
                queueName: QueueName = QueueName(s"queue-${Random.nextInt(1000)}"))(
        implicit executionContext: ExecutionContext,
        ioMonadError: MonadError[IO, Throwable]): Stream[IO, ListBuffer[Delivery]] = {
      val stubConsumeHandler = new StubConsumeHandler[IO, Delivery]()(ioMonadError)

      Stream
        .eval(IO(stubConsumeHandler.receivedMessages))
        .concurrently(
          Stream
            .eval(IO {
              val testDeclaration = List(
                Queue(queueName),
                Exchange(exchangeName, exchangeType = Topic)
                  .binding(routingKey -> queueName)
              )
              logger.info(s"Defining a consumer with the follow config: [$exchangeName -> $routingKey -> $queueName]")
              DeclarationExecutor(testDeclaration, amqpClient)
            })
            .flatMap(_ => amqpClient.consumer(queueName, stubConsumeHandler)))
    }
  }

  def amqpOpsFor(addBinding: Binding => Try[Unit]): AmqpOps =
    new AmqpOps {
      override def declareExchange(exchange: Exchange): Try[Unit] = Try(())

      override def bindQueue(binding: Binding): Try[Unit] = addBinding(binding)

      override def declareQueue(queue: Queue): Try[Unit] = Try(())

      override def purgeQueue(name: QueueName): Try[Unit] = Try(())

      override def bindExchange(binding: ExchangeBinding): Try[Unit] = Try(())
    }

  def withSimulator[P](declarations: Iterable[Declaration] = List.empty)(ports: Fs2AmqpSimulator => Stream[IO, P])(
      test: P => IO[Assertion])(implicit executionContext: ExecutionContext,
                                ioMonadError: MonadError[IO, Throwable],
                                futureMonad: MonadError[Future, Throwable]): Unit = {
    val amqpClient = rabbitSimulator
    val p = for {
      halted <- Stream.eval(async.signalOf[IO, Boolean](false))
      _      <- Stream.eval(IO(DeclarationExecutor(declarations, amqpClient)))
      ports  <- ports(amqpClient).interruptWhen(halted)
      _      <- Stream.eval(test(ports))
      _      <- Stream.eval(halted.set(true))
    } yield ()
    p.compile.last.unsafeRunSync()
  }

  def withMemorySimulator[P](declarations: Iterable[Declaration] = List.empty,
                             config: MemoryAmqpSimulator.Config = MemoryAmqpSimulator.Config.default)(
      ports: MemoryAmqpSimulator[IO] => Stream[IO, P])(test: P => IO[Assertion])(
      implicit executionContext: ExecutionContext,
      ioMonadError: MonadError[IO, Throwable]): Unit =
    Scheduler[IO](2)
      .flatMap { implicit s =>
        for {
          amqpClient <- Stream.eval(memorySimulator(config))
          halted     <- Stream.eval(async.signalOf[IO, Boolean](false))
          _          <- Stream.eval(IO(DeclarationExecutor(declarations, amqpClient)))
          ports      <- ports(amqpClient).interruptWhen(halted)
          _          <- Stream.eval(test(ports))
          _          <- Stream.eval(halted.set(true))
        } yield ()

      }
      .compile
      .last
      .unsafeRunSync()
}
