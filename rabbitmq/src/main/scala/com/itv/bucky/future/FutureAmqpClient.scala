package com.itv.bucky.future

import java.util.concurrent.{Executors, TimeUnit}

import com.itv.bucky._
import com.rabbitmq.client.{Channel => RabbitChannel}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

abstract class FutureAmqpClient[M[_]](channelFactory: M[RabbitChannel])(implicit M: Monad[M], executionContext: ExecutionContext) extends AmqpClient[M, Future, Throwable, Unit] with StrictLogging {

  override def consumer(queueName: QueueName, handler: Handler[Future, Delivery], actionOnFailure: ConsumeAction = DeadLetter, prefetchCount: Int = 0): M[Unit] =
    M.flatMap(channelFactory) { (channel: RabbitChannel) =>
      M.apply{
        val consumer = Consumer.defaultConsumer(channel, queueName, handler, actionOnFailure)
        Consumer[Future, Throwable](channel, queueName, consumer, prefetchCount)
      }
    }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): M[Publisher[Future, PublishCommand]] =
    M.flatMap(channelFactory) { channel: RabbitChannel =>
      M.apply(
        Try {
          logger.info(s"Creating publisher")

          val unconfirmedPublications = Publisher.confirmListener[Promise[Unit]](channel) //
          {
            _.success(())
          } //
          { (p, e) =>
            p.failure(e)
          }

          publisherWrapperLifecycle(timeout) { cmd =>
            val promise = Promise[Unit]()
            Publisher.publish(channel, cmd, promise, unconfirmedPublications) { (t, e) => t.failure(e) }
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


  private def publisherWrapperLifecycle(timeout: Duration): Publisher[Future, PublishCommand] => Publisher[Future, PublishCommand] = timeout match {
    case finiteTimeout: FiniteDuration =>
      new FutureTimeoutPublisher(_, finiteTimeout)(Executors.newSingleThreadScheduledExecutor())
    case _ => identity
  }
}
