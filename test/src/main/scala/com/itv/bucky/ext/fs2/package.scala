package com.itv.bucky.ext

import _root_.fs2._
import cats.Show
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

  sealed trait ConsumeActionResult

  object ConsumeActionResult {

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

    def notBindingFound: ConsumeActionResult  = UnableToBeConsumed(publishCommand, "Not binding found!")
    def notConsumerFound: ConsumeActionResult = UnableToBeConsumed(publishCommand, "Not consumer found!")
  }

  protected[fs2] case class Message(publishCommand: PublishCommand, promise: async.Promise[IO, ConsumeActionResult])

  protected[fs2] object Message {

    case class Data(message: Message, retries: Int, source: Source)

    implicit val showInstance: Show[Message] = (m: Message) =>
      s"[${m.publishCommand.body.toString}] {${m.publishCommand.exchange}-> ${m.publishCommand.routingKey}} "

    sealed trait Source
    object Source {
      case object Original        extends Source
      case object NoBindingFound  extends Source
      case object NoConsumerFound extends Source
    }

    def noBindingFound(message: Message.Data): Message.Data =
      message
        .copy(
          retries = message.retries + 1,
          source = Message.Source.NoBindingFound
        )
    def noConsumerFound(message: Message.Data): Message.Data =
      message
        .copy(
          retries = message.retries + 1,
          source = Message.Source.NoConsumerFound
        )
  }

  type Fs2AmqpSimulator = AmqpSimulator[Id, IO, Throwable, Stream[IO, Unit]]

  def rabbitSimulator(implicit executionContext: ExecutionContext,
                      ioMonadError: MonadError[IO, Throwable],
                      futureMonad: MonadError[Future, Throwable]): Fs2AmqpSimulator =
    old.rabbitSimulator

  def memorySimulator(maxRetries: Int = 10)(implicit executionContext: ExecutionContext,
                                            scheduler: Scheduler,
                                            idMonad: Monad[Id],
                                            ioMonadError: MonadError[IO, Throwable],
                                            F: Sync[IO]): IO[MemoryAmqpSimulator[IO]] =
    io.apply(maxRetries)

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

  def withMemorySimulator[P](declarations: Iterable[Declaration] = List.empty)(
      ports: MemoryAmqpSimulator[IO] => Stream[IO, P])(test: P => IO[Assertion])(
      implicit executionContext: ExecutionContext,
      ioMonadError: MonadError[IO, Throwable]): Unit =
    Scheduler[IO](2)
      .flatMap { implicit s =>
        for {
          amqpClient <- Stream.eval(memorySimulator())
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
