package com.itv.bucky

import com.itv.bucky.pattern.requeue.RequeuePolicy

import scala.language.higherKinds
import scala.util.Random

object Any {

  def randomPayload() =
    Payload.from(randomString())


  def randomString() =
    s"Hello World ${new Random().nextInt(10000)}! "


  def randomQueue() =
    QueueName(s"bucky-queue-${new Random().nextInt(10000)}")


  def getHeader(header: String, properties: MessageProperties): Option[String] =
    properties.headers.get(header).map(_.toString)

}


sealed trait RequeueStrategy[+F[_]]

case object NoneHandler extends RequeueStrategy[Nothing]

case class NoneRequeue[F[_]](handler: Handler[F, Delivery]) extends RequeueStrategy[F]

case class SimpleRequeue[F[_]](handler: Handler[F, Delivery]) extends RequeueStrategy[F]

case class RawRequeue[F[_]](handler: RequeueHandler[F, Delivery], requeuePolicy: RequeuePolicy) extends RequeueStrategy[F]

case class TypeRequeue[F[_], T](handler: RequeueHandler[F, T], requeuePolicy: RequeuePolicy, unmarshaller: PayloadUnmarshaller[T])
  extends RequeueStrategy[F]
