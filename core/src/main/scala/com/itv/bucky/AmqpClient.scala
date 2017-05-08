package com.itv.bucky

import java.util.concurrent.TimeUnit

import com.itv.bucky.decl._
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try
import scala.language.higherKinds

trait AmqpClient[B[_], F[_], E] {

  def publisherOf[T](builder: PublishCommandBuilder[T], timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS))
                    (implicit M:Monad[B], F: MonadError[F, E]): B[Publisher[F, T]] = {
    M.map(publisher(timeout))(p => AmqpClient.publisherOf(builder)(p)(F))
  }

  def publisher(timeout: Duration = FiniteDuration(10, TimeUnit.SECONDS)): B[Publisher[F, PublishCommand]]

  def consumer(queueName: QueueName, handler: Handler[F, Delivery], exceptionalAction: ConsumeAction = DeadLetter, prefetchCount: Int = 0): B[Unit]

  def performOps(thunk: AmqpOps => Try[Unit]): Try[Unit]

  def estimatedMessageCount(queueName: QueueName): Try[Int]

}

trait AmqpOps {
  def declareQueue(queue: Queue): Try[Unit]
  def declareExchange(echange: Exchange): Try[Unit]
  def bindQueue(binding: Binding): Try[Unit]
  def purgeQueue(name: QueueName): Try[Unit]
}


object AmqpClient extends StrictLogging {

  def publisherOf[F[_], T](commandBuilder: PublishCommandBuilder[T])(publisher: Publisher[F, PublishCommand])
                    (implicit M: Monad[F]): Publisher[F, T] = (message: T) =>
       M.flatMap(M.apply {
        commandBuilder.toPublishCommand(message)
      }){ publisher }


  def deliveryHandlerOf[F[_], T](handler: Handler[F, T], unmarshaller: DeliveryUnmarshaller[T], unmarshalFailureAction: ConsumeAction = DeadLetter)(implicit monad: Monad[F]): Handler[F, Delivery] =
    new DeliveryUnmarshalHandler[F, T, ConsumeAction](unmarshaller)(handler, unmarshalFailureAction)

  def handlerOf[F[_], T](handler: Handler[F, T], unmarshaller: PayloadUnmarshaller[T], unmarshalFailureAction: ConsumeAction = DeadLetter)
                        (implicit monad: Monad[F]): Handler[F, Delivery] =
    deliveryHandlerOf(handler, Unmarshaller.toDeliveryUnmarshaller(unmarshaller), unmarshalFailureAction)

}