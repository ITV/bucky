package com.itv.bucky.future

import com.itv.bucky.template.ChannelAmqpPurgeTest

import scala.concurrent.Future

class FutureChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[Future] with FuturePublisherTest {}
