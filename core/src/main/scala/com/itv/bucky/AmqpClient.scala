package com.itv.bucky

import java.util.{Collections, UUID}
import java.util.concurrent.{AbstractExecutorService, TimeUnit}

import cats.effect._
import cats.effect.implicits._
import cats.effect.concurrent.Ref
import cats.implicits._
import com.itv.bucky.consume.{ConsumeAction, DeadLetter, Delivery}
import com.rabbitmq.client.{ConnectionFactory, ShutdownSignalException, Channel => RabbitChannel, Connection => RabbitConnection}
import com.itv.bucky.decl._
import com.itv.bucky.publish.PublishCommand
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.language.higherKinds
import org.typelevel.log4cats.Logger

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

object AmqpClient{

  private def createChannel[F[_]](connection: RabbitConnection)(implicit F: Sync[F], cs: ContextShift[F], logger: Logger[F]): Resource[F, RabbitChannel] = {
    for {
      _       <- Resource.liftF[F, Unit](logger.info(s"Starting Channel"))
      channel <- Resource.fromAutoCloseable(Sync[F].delay(connection.createChannel())).onFinalizeCase {
          case ExitCase.Completed => 
            logger.info("Channel shutdown")
          case ExitCase.Error(cause : ShutdownSignalException) if cause.isInitiatedByApplication => 
            logger.info(s"Channel shut down due to explicit application action: ${cause.getMessage}")
          case ExitCase.Error(cause) => 
            logger.error(cause)(s"Channel shut down by broker or because of detectable non-deliberate application failure")
          case ExitCase.Canceled =>
            logger.info("Channel shutdown (cancelled)")
      }
    } yield channel
  }

  private def createConnection[F[_]](
      config: AmqpClientConfig)(implicit F: Sync[F], cs: ContextShift[F], executionContext: ExecutionContext, logger: Logger[F]): Resource[F, RabbitConnection] = {
    val make =
      F.delay {
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
            logger.error(exception)(s"Failure when starting AmqpClient because ${exception.getMessage}").pure[F]
        }
        .rethrow

    Resource.make(cs.shift.flatMap(_ => make))(connection => F.delay(connection.close()))
  }

  def apply[F[_]](config: AmqpClientConfig)(implicit F: ConcurrentEffect[F],
                                            cs: ContextShift[F],
                                            t: Timer[F],
                                            executionContext: ExecutionContext,
                                            logger:Logger[F]): Resource[F, AmqpClient[F]] =
    for {
      connection <- createConnection(config)
      publishChannel = createChannel(connection).map(Channel.apply[F])
      buildChannel   = () => createChannel(connection).map(Channel.apply[F])
      client <- apply[F](config, buildChannel, publishChannel)
    } yield client

  def apply[F[_]](config: AmqpClientConfig, buildChannel: () => Resource[F, Channel[F]], publishChannel: Resource[F, Channel[F]])(
      implicit F: ConcurrentEffect[F],
      cs: ContextShift[F],
      t: Timer[F],
      logger: Logger[F],
      executionContext: ExecutionContext): Resource[F, AmqpClient[F]] =
    publishChannel.flatMap { channel =>
      val make =
        for {
          _                 <- cs.shift
          connectionManager <- AmqpClientConnectionManager(config, channel)
        } yield mkClient(buildChannel, connectionManager)
      Resource.make(make)(_ => F.unit)
    }

  private def mkClient[F[_]](
      buildChannel: () => Resource[F, Channel[F]],
      connectionManager: AmqpClientConnectionManager[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F], t: Timer[F], logger: Logger[F]): AmqpClient[F] =
    new AmqpClient[F] {
      private def repeatUntil[A](eval: F[A])(pred: A => Boolean)(sleep: FiniteDuration): F[Unit] =
        for {
          result <- eval
          ended  <- F.pure(pred(result))
          _      <- if (ended) F.unit else t.sleep(sleep) *> repeatUntil(eval)(pred)(sleep)
        } yield ()

      override def publisher(): Publisher[F, PublishCommand] = cmd => {
        for {
          _ <- cs.shift
          _ <- connectionManager.publish(cmd)
        } yield ()
      }

      override def registerConsumer(queueName: QueueName,
                                    handler: Handler[F, Delivery],
                                    exceptionalAction: ConsumeAction,
                                    prefetchCount: Int,
                                    shutdownTimeout: FiniteDuration,
                                    shutdownRetry: FiniteDuration): Resource[F, Unit] =
        for {
          channel <- buildChannel()
          handling <- Resource.make(Ref.of[F, Set[UUID]](Set.empty))(set =>
            repeatUntil(logger.debug("Verifying running handlers.") *> set.get)(_.isEmpty)(shutdownRetry).timeout(shutdownTimeout))
          newHandler = (delivery: Delivery) =>
            for {
              id            <- F.delay(UUID.randomUUID())
              _             <- handling.update(set => set + id)
              resultAttempt <- handler(delivery).attempt
              _             <- handling.update(set => set - id)
              result        <- F.fromEither(resultAttempt)
            } yield result
          result <- Resource.liftF(connectionManager.registerConsumer(channel, queueName, newHandler, exceptionalAction, prefetchCount))
        } yield result

      override def declare(declarations: Declaration*): F[Unit]          = connectionManager.declare(declarations)
      override def declare(declarations: Iterable[Declaration]): F[Unit] = connectionManager.declare(declarations)

      override def isConnectionOpen: F[Boolean] = connectionManager.publishChannel.isConnectionOpen
    }
}
