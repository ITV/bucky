package com.itv.bucky.future

import java.util.concurrent.{Executors, TimeUnit}

import com.itv.bucky._
import com.rabbitmq.client.{Channel => RabbitChannel}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

abstract class FutureAmqpClient[B[_]](channelFactory: B[RabbitChannel])(implicit executionContext: ExecutionContext)
    extends AmqpClient[B, Future, Throwable, Unit]
    with StrictLogging {

  override implicit def effectMonad = futureMonad(executionContext)

  override def consumer(queueName: QueueName,
                        handler: Handler[Future, Delivery],
                        actionOnFailure: ConsumeAction = DeadLetter,
                        prefetchCount: Int = 0): B[Unit] =
    monad.flatMap(channelFactory) { (channel: RabbitChannel) =>
      monad.apply {
        val consumer = Consumer.defaultConsumer(channel, queueName, handler, actionOnFailure)
        Consumer[Future, Throwable](channel, queueName, consumer, prefetchCount)
      }
    }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): B[Publisher[Future, PublishCommand]] =
    monad.flatMap(channelFactory) { channel: RabbitChannel =>
      monad.apply(
        Try {
          logger.info(s"Creating publisher")
          val channelPublisher = ChannelPublisher(channel)

          val unconfirmedPublications = channelPublisher.confirmListener[Promise[Unit]] //
          {
            _.success(())
          } //
          { (p, e) =>
            p.failure(e)
          }

          publisherWrapperLifecycle(timeout) { cmd =>
            val promise = Promise[Unit]()
            channelPublisher.publish(cmd, promise, unconfirmedPublications) { (t, e) =>
              t.failure(e)
            }
            promise.future
          }
        } match {
          case Success(publisher) =>
            logger.info(s"Publisher has been created successfully!")
            publisher
          case Failure(exception) =>
            logger.error(s"Error when creating publisher because ${exception.getMessage}", exception)
            throw exception

        }
      )
    }

  private def publisherWrapperLifecycle(
      timeout: Duration): Publisher[Future, PublishCommand] => Publisher[Future, PublishCommand] = timeout match {
    case finiteTimeout: FiniteDuration =>
      new FutureTimeoutPublisher(_, finiteTimeout)(Executors.newSingleThreadScheduledExecutor())
    case _ => identity
  }
}
