package com.itv.bucky

import cats.effect.implicits._
import cats.effect.std.Dispatcher
import cats.effect._
import cats.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery}
import com.itv.bucky.decl._
import com.itv.bucky.publish.PublishCommand
import com.rabbitmq.client.{ConnectionFactory, ShutdownListener, ShutdownSignalException, Channel => RabbitChannel, Connection => RabbitConnection}
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.{AbstractExecutorService, Executors, TimeUnit}
import java.util.{Collections, UUID}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.language.higherKinds
import scala.util.Random

trait AmqpClient[F[_]] {
  def declare(declarations: Declaration*): F[Unit]
  def declare(declarations: Iterable[Declaration]): F[Unit]
  def publisher(): Publisher[F, PublishCommand]
  def registerConsumer(queueName: QueueName,
                       handler: Handler[F, Delivery],
                       exceptionalAction: ConsumeAction = DeadLetter,
                       prefetchCount: Int = 1,
                       shutdownTimeout: FiniteDuration = 1.minutes,
                       shutdownRetry: FiniteDuration = 500.millis): Resource[F, Unit]
  def isConnectionOpen: F[Boolean]
}

object AmqpClient extends StrictLogging {

  private def createChannel[F[_]](connection: RabbitConnection)(implicit F: Async[F]): Resource[F, RabbitChannel] = {
    val make =
      F.blocking {
          logger.info(s"Starting Channel")
          val channel = connection.createChannel()
          channel.addShutdownListener(new ShutdownListener {
            override def shutdownCompleted(cause: ShutdownSignalException): Unit =
              if (cause.isInitiatedByApplication)
                logger.info(s"Channel shut down due to explicit application action: ${cause.getMessage}")
              else
                logger.error(s"Channel shut down by broker or because of detectable non-deliberate application failure", cause)
          })
          channel
        }
        .attempt
        .flatTap {
          case Right(_) =>
            F.delay(logger.info(s"Channel has been started successfully!"))
          case Left(exception) =>
            F.delay(logger.error(s"Failure when starting Channel because ${exception.getMessage}", exception))
        }
        .rethrow

    Resource.make(make)(channel => F.blocking(channel.close()))
  }

  private def createConnection[F[_]](
      config: AmqpClientConfig)(implicit F: Async[F], executionContext: ExecutionContext): Resource[F, RabbitConnection] = {
    val make =
      F.blocking {
          logger.info(s"Starting AmqpClient")
          val connectionFactory = new ConnectionFactory()
          connectionFactory.setHost(config.host)
          connectionFactory.setPort(config.port)
          connectionFactory.setUsername(config.username)
          connectionFactory.setPassword(config.password)
          connectionFactory.setAutomaticRecoveryEnabled(config.networkRecoveryInterval.isDefined)
          connectionFactory.setSharedExecutor(executionContext match {
            case null                                  => throw null
            case eces: ExecutionContextExecutorService => eces
            case other =>
              new AbstractExecutorService with ExecutionContextExecutorService {
                override def prepare(): ExecutionContext                             = other
                override def isShutdown                                              = false
                override def isTerminated                                            = false
                override def shutdown()                                              = ()
                override def shutdownNow()                                           = Collections.emptyList[Runnable]
                override def execute(runnable: Runnable): Unit                       = other execute runnable
                override def reportFailure(t: Throwable): Unit                       = other reportFailure t
                override def awaitTermination(length: Long, unit: TimeUnit): Boolean = false
              }
          })
          config.networkRecoveryInterval.map(_.toMillis.toInt).foreach(connectionFactory.setNetworkRecoveryInterval)
          config.virtualHost.foreach(connectionFactory.setVirtualHost)
          connectionFactory.newConnection()
        }
        .attempt
        .flatTap {
          case Right(_) =>
            logger.info(s"AmqpClient has been started successfully!").pure[F]
          case Left(exception) =>
            logger.error(s"Failure when starting AmqpClient because ${exception.getMessage}", exception).pure[F]
        }
        .rethrow

    Resource.make(make)(connection => F.blocking(connection.close()))
  }

  def apply[F[_]](config: AmqpClientConfig)(implicit F: Async[F],
                                            t: Temporal[F],
                                            executionContext: ExecutionContext): Resource[F, AmqpClient[F]] =
    for {
      dispatcher <- Dispatcher[F]
      connection <- createConnection(config)
      singleThreadExecutor <- Resource.eval(F.delay(ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(new Thread(_, s" RabbitChannelThread-${Random.alphanumeric.take(6).mkString}")))))
      publishChannel = createChannel(connection).map(Channel.apply[F](_, dispatcher, singleThreadExecutor))
      buildChannel   = () => createChannel(connection).map(Channel.apply[F](_, dispatcher, singleThreadExecutor))
      client <- apply[F](config, buildChannel, publishChannel, dispatcher)
    } yield client

  def apply[F[_]](config: AmqpClientConfig, buildChannel: () => Resource[F, Channel[F]], publishChannel: Resource[F, Channel[F]], dispatcher: Dispatcher[F])(
      implicit F: Async[F],
      t: Temporal[F],
      executionContext: ExecutionContext): Resource[F, AmqpClient[F]] =
    publishChannel.flatMap { channel =>
      val make =
        for {
          connectionManager <- AmqpClientConnectionManager(config, channel, dispatcher)
        } yield mkClient(buildChannel, connectionManager)
      Resource.make(make)(_ => F.unit)
    }

  
  private def mkClient[F[_]](
      buildChannel: () => Resource[F, Channel[F]],
      connectionManager: AmqpClientConnectionManager[F])(implicit F: Async[F], t: Temporal[F]): AmqpClient[F] =
    new AmqpClient[F] {
      private def repeatUntil[A](eval: F[A])(pred: A => Boolean)(sleep: FiniteDuration): F[Unit] =
        for {
          result <- eval
          ended  <- F.pure(pred(result))
          _      <- if (ended) F.unit else t.sleep(sleep) *> repeatUntil(eval)(pred)(sleep)
        } yield ()

      override def publisher(): Publisher[F, PublishCommand] = cmd => connectionManager.publish(cmd)


      override def registerConsumer(queueName: QueueName,
                                    handler: Handler[F, Delivery],
                                    exceptionalAction: ConsumeAction,
                                    prefetchCount: Int,
                                    shutdownTimeout: FiniteDuration,
                                    shutdownRetry: FiniteDuration): Resource[F, Unit] =
        for {
          channel <- buildChannel()
          handling <- Resource.make(Ref.of[F, Set[UUID]](Set.empty))(set =>
            repeatUntil(F.blocking(logger.debug("Verifying running handlers.")) *> set.get)(_.isEmpty)(shutdownRetry).timeout(shutdownTimeout))
          newHandler = (delivery: Delivery) =>
            for {
              id            <- F.delay(UUID.randomUUID())
              _             <- handling.update(set => set + id)
              resultAttempt <- handler(delivery).attempt
              _             <- handling.update(set => set - id)
              result        <- F.fromEither(resultAttempt)
            } yield result
          result <- Resource.eval(connectionManager.registerConsumer(channel, queueName, newHandler, exceptionalAction, prefetchCount))
        } yield result

      override def declare(declarations: Declaration*): F[Unit]          = connectionManager.declare(declarations)
      override def declare(declarations: Iterable[Declaration]): F[Unit] = connectionManager.declare(declarations)

      override def isConnectionOpen: F[Boolean] = connectionManager.publishChannel.isConnectionOpen
    }
}
