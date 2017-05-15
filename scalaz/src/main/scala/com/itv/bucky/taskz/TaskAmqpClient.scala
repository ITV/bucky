package com.itv.bucky.taskz

import java.util.concurrent.ExecutorService

import com.itv.bucky.Monad.Id
import com.itv.bucky.{Channel, _}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{DefaultConsumer, Envelope, Channel => RabbitChannel, Connection => RabbitConnection, Consumer => RabbitMqConsumer}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.Try
import scalaz.{-\/, \/, \/-}
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.Process

case class TaskAmqpClient(channel: Id[RabbitChannel])(implicit pool: ExecutorService = Strategy.DefaultExecutorService)  extends AmqpClient[Id, Task, Throwable, Process[Task, Unit]]
  with StrictLogging {
  type Register = (\/[Throwable, Unit]) => Unit

  override def publisher(timeout: Duration): Id[Publisher[Task, PublishCommand]] = {
    logger.info(s"Creating publisher")
    val handleFailure = (f: Register, e: Exception) => f.apply(-\/(e))
    val pendingConfirmations = Publisher.confirmListener[Register](channel) {
      _.apply(\/-(()))
    }(handleFailure)

    cmd =>
      Task.async { pendingConfirmation: Register =>
        Publisher.publish[Register](channel, cmd, pendingConfirmation, pendingConfirmations)(handleFailure)
      }.timed(timeout)
  }

  override def consumer(queueName: QueueName, handler: Handler[Task, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): Id[Process[Task, Unit]] = {
    import scalaz.stream.async
    val messages = async.unboundedQueue[Delivery]

    def createConsumer: Task[RabbitMqConsumer] = Task {
      val consumer: RabbitMqConsumer = new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
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
    val sink: Sink[Task, Delivery] = Process repeatEval Task(processMessage _)

    (source to sink)
  }

  override def performOps(thunk: (AmqpOps) => Try[Unit]): Try[Unit] = thunk(ChannelAmqpOps(channel))

  override def estimatedMessageCount(queueName: QueueName): Try[Int] = Channel.estimateMessageCount(channel, queueName)

}


object TaskAmqpClient extends StrictLogging {

  import Monad._

  def connection(config: AmqpClientConfig)(implicit pool: ExecutorService): Task[RabbitConnection] = Task.delay {
    Connection(config)
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

  def closeChannel(channel: RabbitChannel)(implicit pool: ExecutorService)= Task.delay {
    Channel.close(channel)
  }

  def fromConnection(connection: RabbitConnection)(implicit pool: ExecutorService = Strategy.DefaultExecutorService): TaskAmqpClient = Channel(connection).flatMap(TaskAmqpClient(_))

  def fromConfig(config: AmqpClientConfig)(implicit pool: ExecutorService = Strategy.DefaultExecutorService): TaskAmqpClient = fromConnection(Connection(config))
}


object ProcessAmqpClient extends StrictLogging {

  def fromConfig(config: AmqpClientConfig)(f: TaskAmqpClient => Process[Task, Unit])(implicit pool: ExecutorService = Strategy.DefaultExecutorService): Process[Task, Unit] = Process
    .bracket(TaskAmqpClient.connection(config))(closeConnection)(fromConnection(_)(f))

  def fromConnection(connection: RabbitConnection)(f: TaskAmqpClient => Process[Task, Unit])(implicit pool: ExecutorService = Strategy.DefaultExecutorService): Process[Task, Unit] = Process
    .bracket(TaskAmqpClient.channel(connection).map(TaskAmqpClient.apply)) { amqpClient =>
      closeChannel(amqpClient.channel)
    }(f)

  private def closeConnection(connection: RabbitConnection)(implicit pool: ExecutorService = Strategy.DefaultExecutorService) = Process eval_ TaskAmqpClient.closeConnection(connection)

  private def closeChannel(channel: RabbitChannel)(implicit pool: ExecutorService = Strategy.DefaultExecutorService) = Process eval_ TaskAmqpClient.closeChannel(channel)

}
