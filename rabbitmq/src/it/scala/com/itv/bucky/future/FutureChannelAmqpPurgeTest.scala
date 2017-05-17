package com.itv.bucky.future

import com.itv.bucky.ChannelAmqpPurgeTest

import scala.concurrent.Future

class FutureChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[Future] with FuturePublisherTest {

}