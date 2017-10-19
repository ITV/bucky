package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.fs2.utils.{IOEffectMonad, IOEffectVerification, IOPublisherConsumerBaseTest}
import com.itv.bucky.template._

class IORequeueIntegrationTest
    extends RequeueIntegrationTest[IO]
    with IOEffectVerification
    with IOEffectMonad
    with IOPublisherConsumerBaseTest {}
