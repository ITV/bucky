package com.itv.bucky

import cats.effect.IO
import com.itv.bucky.stub.StubChannel
import com.rabbitmq.client.Channel
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.mockito.MockitoSugar._

class AmqpClientConnectionManagerTest extends FunSuite with Matchers {
  test("should cleanupt pending confirmations after a timeout occurs") {

    val channel = new StubChannel
    val manager = new AmqpClientConnectionManager[IO](
      AmqpClientConfig("",1,"", ""),
      channel,
      channel.getConnection,

    )
    for {

    }
  }
}
