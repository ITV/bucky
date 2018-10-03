package com.itv.bucky.taskz

import java.util.concurrent.ExecutorService

import com.itv.bucky.Monad._
import com.itv.bucky.taskz.AbstractTaskAmqpClient.TaskConsumer
import com.itv.bucky.{Channel, _}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{
  DefaultConsumer,
  Envelope,
  Channel => RabbitChannel,
  Connection => RabbitConnection,
  Consumer => RabbitMqConsumer
}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.Try
import scalaz.{-\/, \/, \/-}
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.async.mutable.Signal
import scalaz.stream.{Process, async, wye}

object AbstractTaskAmqpClient {

  type TaskConsumer   = Id[Process[Task, Unit]]
  type TaskAmqpClient = AmqpClient[Id, Task, Throwable, TaskConsumer]

}

case class TaskAmqpClient(channel: Id[RabbitChannel])(implicit pool: ExecutorService = Strategy.DefaultExecutorService)
    extends AbstractTaskAmqpClient.TaskAmqpClient
    with StrictLogging {
  type Register = (\/[Throwable, Unit]) => Unit

  override implicit def monad: Monad[Id] = idMonad

  override implicit def effectMonad: MonadError[Task, Throwable] = taskMonadError(pool)

  override def publisher(timeout: Duration): Id[Publisher[Task, PublishCommand]] = {
    logger.info(s"Creating publisher")
    val handleFailure    = (f: Register, e: Exception) => f.apply(-\/(e))
    val channelPublisher = ChannelPublisher(channel)
    val pendingConfirmations = channelPublisher.confirmListener[Register] {
      _.apply(\/-(()))
    }(handleFailure)

    cmd =>
      Task
        .async { pendingConfirmation: Register =>
          channelPublisher.publish[Register](cmd, pendingConfirmation, pendingConfirmations)(handleFailure)
        }
        .timed(timeout)
  }

  override def consumer(queueName: QueueName,
                        handler: Handler[Task, Delivery],
                        actionOnFailure: ConsumeAction = DeadLetter,
                        prefetchCount: Int = 0): TaskConsumer = {
    import scalaz.stream.async
    val messages = async.unboundedQueue[Delivery]

    def createConsumer: Task[RabbitMqConsumer] = Task {
      val consumer: RabbitMqConsumer = new DefaultConsumer(channel) {
        logger.info(s"Creating consumer for $queueName")
        override def handleDelivery(consumerTag: String,
                                    envelope: Envelope,
                                    properties: BasicProperties,
                                    body: Array[Byte]): Unit = {
          val delivery = Consumer.deliveryFrom(consumerTag, envelope, properties, body)
          messages.enqueueOne(delivery).unsafePerformAsync {
            case \/-(_) =>
            case -\/(exception) =>
              logger.error(s"Not able to enqueue $delivery because ${exception.getMessage}", exception)
              Consumer.requeueImmediately(channel, delivery)
          }
        }
      }
      Consumer[Task, Throwable](channel, queueName, consumer, prefetchCount)
      consumer
    }

    def processMessage(delivery: Delivery): Task[Unit] =
      Consumer.processDelivery(channel, queueName, handler, actionOnFailure, delivery)

    import scalaz.stream._

    val source: Process[Task, Delivery] = (Process eval createConsumer) flatMap (_ => messages.dequeue)
    val sink: Sink[Task, Delivery]      = Process repeatEval Task(processMessage _)

    (source to sink)
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = Channel.estimateMessageCount(channel, queueName)

}

object TaskAmqpClient extends StrictLogging {

  import Monad._
  import scala.concurrent.duration._

  type Closeable =  AmqpClient.WithCloseable[Id, Task, Throwable, Process[Task, Unit]]

  def connection(config: AmqpClientConfig)(implicit pool: ExecutorService): Task[RabbitConnection] = {
    val value = Task.delay {
      Connection(config)
    }
    config.networkRecoveryIntervalOnStart.fold(value) { networkRecoveryOnStart =>
      logger.debug(s"Number of retries: ${networkRecoveryOnStart.numberOfRetries}")

      val retries: Seq[Duration] =
        (1L to networkRecoveryOnStart.numberOfRetries).map(_ => networkRecoveryOnStart.interval)
      value.retry(retries)
    }
  }

  def channel(connection: RabbitConnection)(implicit pool: ExecutorService): Task[RabbitChannel] = Task.delay {
    Channel(connection)
  }

  def closeAll(client: TaskAmqpClient)(implicit pool: ExecutorService = Strategy.DefaultExecutorService) = {
    val connection = client.channel.getConnection
    for {
      _ <- closeChannel(client.channel)
      _ <- closeConnection(connection)
    } yield ()
  }

  def closeConnection(connection: RabbitConnection)(implicit pool: ExecutorService) =
    Task.delay {
      Connection.close(connection)
    }

  def closeChannel(channel: RabbitChannel)(implicit pool: ExecutorService) = Task.delay {
    Channel.close(channel)
  }

  def fromConnection(connection: RabbitConnection)(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService): TaskAmqpClient = Channel(connection).flatMap {
    channel =>
      val client = TaskAmqpClient(channel)
      sys.addShutdownHook {
        TaskAmqpClient.closeAll(client).unsafePerformSync
      }
      client
  }

  def fromConfig(config: AmqpClientConfig)(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService): TaskAmqpClient =
    fromConnection(connection(config).unsafePerformSync)


  def closeableClient(config: AmqpClientConfig)(
    implicit pool: ExecutorService = Strategy.DefaultExecutorService): TaskAmqpClient.Closeable = {
    val client = fromConfig(config)
    AmqpClient.WithCloseable(client, closeAll(client))
  }
}

object ProcessAmqpClient extends StrictLogging {
  // Refactor the safeBracket to provide a task to close

  def fromConfig(config: AmqpClientConfig)(f: TaskAmqpClient => Process[Task, Unit])(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService): Process[Task, Unit] =
    safeBracket(for {
      connection <- TaskAmqpClient.connection(config)
      channel    <- TaskAmqpClient.channel(connection)
    } yield TaskAmqpClient(channel))(closeAll)(f)

  def fromConnection(connection: RabbitConnection)(f: TaskAmqpClient => Process[Task, Unit])(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService): Process[Task, Unit] =
    safeBracket(
      TaskAmqpClient
        .channel(connection)
        .map(TaskAmqpClient.apply))(closeChannel)(f)

  private def safeBracket[A, O](req: Task[A])(release: A => Task[Unit])(
      rcv: A => Process[Task, O]): Process[Task, O] = {
    val halted = async.signalOf[Boolean](false)
    Process
      .bracket(req) { a =>
        Process eval_ release(a).flatMap(_ => halted.set(true))
      } { a =>
        safeShutdown(rcv(a), halted)
      }
  }

  private def safeShutdown[O](process: Process[Task, O], halted: Signal[Boolean]) = {
    val shutdownRequested = async.signalOf[Boolean](false)

    val requestShutdown =
      shutdownRequested.set(true)

    sys.addShutdownHook {
      requestShutdown.unsafePerformAsync(_ => ())
      halted.discrete.takeWhile(_ == false).run.unsafePerformSync
    }
    shutdownRequested.discrete
      .wye(process)(wye.interrupt)
  }

  private def closeChannel(amqpClient: TaskAmqpClient)(
      implicit pool: ExecutorService = Strategy.DefaultExecutorService) =
    TaskAmqpClient.closeChannel(amqpClient.channel)

  private def closeAll(amqpClient: TaskAmqpClient)(implicit pool: ExecutorService = Strategy.DefaultExecutorService) =
    TaskAmqpClient.closeAll(amqpClient)
}
