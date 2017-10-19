package com.itv.bucky

import com.itv.bucky.decl.Queue
import com.itv.bucky.suite.EffectVerification
import com.typesafe.config.ConfigFactory
import org.scalatest.Assertion
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.concurrent.duration._

package object utils {

  def defaultDeclaration(queueName: QueueName): List[Queue] =
    List(queueName).map(Queue(_).autoDelete.expires(2.minutes))

  def config: AmqpClientConfig = {
    val config = ConfigFactory.load("bucky")
    val host   = config.getString("rmq.host")

    AmqpClientConfig(config.getString("rmq.host"),
                     config.getInt("rmq.port"),
                     config.getString("rmq.username"),
                     config.getString("rmq.password"))
  }

  trait FutureEffectVerification extends EffectVerification[Future] with ScalaFutures {

    def verifySuccess(f: Future[Unit]): Assertion = f.futureValue should ===(())
    def verifyFailure(f: Future[Unit]): Assertion = f.eitherValue shouldBe 'left

  }

}
