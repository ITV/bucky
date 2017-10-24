package com.itv.bucky.fs2

import cats.effect.IO
import com.itv.bucky.fs2.utils.{IOEffectMonad, IOEffectVerification, IOPublisherConsumerBaseTest}
import com.itv.bucky.suite.ConsumerIntegrationTest

class IOConsumerIntegrationTest
    extends ConsumerIntegrationTest[IO]
    with IOEffectVerification
    with IOEffectMonad
    with IOPublisherConsumerBaseTest {}
