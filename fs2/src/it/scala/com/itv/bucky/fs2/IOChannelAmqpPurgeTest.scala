package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.fs2.utils.{IOEffectVerification, IOPublisherBaseTest}
import com.itv.bucky.template.ChannelAmqpPurgeTest

class IOChannelAmqpPurgeTest extends ChannelAmqpPurgeTest[IO] with IOPublisherBaseTest with IOEffectVerification {}
