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
}

object HeaderExt {
  def apply(header: String, properties: MessageProperties): Option[String] =
    properties.headers.get(header).map(_.toString)
}


