package com.itv.bucky.ext.fs2.supersync
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{IO, Sync}
import com.itv.bucky.Monad.Id
import com.itv.bucky._
import com.itv.bucky.decl._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Random, Try}
import cats.syntax.traverse._
import cats.instances.list._
import _root_.fs2.Stream

class SuperSyncSimulator(implicit idMonad: Monad[Id],
                         ioMonadError: MonadError[IO, Throwable])
  extends AmqpClient[Id, IO, Throwable, Stream[IO, Unit]]
    with StrictLogging {

  val bindings = new ListBuffer[Binding]
  val consumers  = new scala.collection.mutable.HashMap[QueueName, Handler[IO, Delivery]]()
  val deliveryTagCounter = new AtomicInteger()

  override implicit def effectMonad: MonadError[IO, Throwable] = ioMonadError
  override implicit def monad: Monad[Id]                       = idMonad

  def publish(publishCommand: PublishCommand): IO[List[ConsumeAction]] =
    IO {
      val bindingLookup = bindingsFor(publishCommand)(bindings.toList)

      bindingLookup match {
        case Nil => throw new IllegalStateException(s"No binding found for message: $publishCommand")
        case _ =>
          val queueNames = bindingLookup.map(_.queueName)
          val handlers = queueNames.flatMap(consumers.get)//.flatten

          handlers match {
            case Nil => throw new IllegalStateException(s"No handlers found for message: $publishCommand")
            case _ =>
              handlers.traverse(_(deliveryFor(publishCommand))).unsafeRunSync()
          }
      }
    }

  override def publisher(timeout: Duration): Publisher[IO, PublishCommand] =
    publish(_).map(_ => ())

  override def consumer(queueName: QueueName,
                        handler: Handler[IO, Delivery],
                        exceptionalAction: ConsumeAction,
                        prefetchCount: Int): Stream[IO, Unit] = {
    if (!consumers.contains(queueName))
      consumers.put(queueName, handler)
    else
      throw new IllegalStateException(s"Duplicate consumer started for ${queueName.value}")

    /*
    effectful things happening separately as we want to ensure consumers are registered eagerly
     */
    Stream.emit(())
  }

  import com.itv.bucky.ext.fs2.amqpOpsFor

  override def performOps(thunk: AmqpOps => Try[Unit]): Try[Unit] =
    thunk(amqpOpsFor(x =>
      Try {
        bindings += x
        ()
      }))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = Try(0)

  private def bindingsFor(publishCommand: PublishCommand): List[Binding] => List[Binding] = _.filter { b =>
    publishCommand.routingKey == b.routingKey &&
      publishCommand.exchange == b.exchangeName
  }

  private def deliveryFor(publishCommand: PublishCommand): Delivery =
    Delivery(
      publishCommand.body,
      ConsumerTag("ctag"),
      Envelope(deliveryTagCounter.incrementAndGet(), redeliver = false, publishCommand.exchange, publishCommand.routingKey),
      publishCommand.basicProperties
    )

  def consume(exchangeName: ExchangeName,
                  routingKey: RoutingKey,
                  queueName: QueueName = QueueName(s"queue-${Random.nextInt(1000)}"))(
                   implicit ioMonadError: MonadError[IO, Throwable]
                 ): Stream[IO, ListBuffer[Delivery]] = {
    val stubConsumeHandler            = new StubConsumeHandler[IO, Delivery]()(ioMonadError)
    val testDeclaration = List(
      Queue(queueName),
      Exchange(exchangeName, exchangeType = Topic)
        .binding(routingKey -> queueName)
    )
    logger.info(s"Defining a consumer with the follow config: [$exchangeName -> $routingKey -> $queueName]")
    DeclarationExecutor(testDeclaration, this)

    for {
      _ <- consumer(queueName, stubConsumeHandler)
    }
      yield stubConsumeHandler.receivedMessages
  }

}
