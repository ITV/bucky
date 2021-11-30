package com.itv.bucky.test

import cats.effect.testing.scalatest.{AssertingSyntax, EffectTestSupport}
import cats.effect.unsafe.IORuntime
import org.scalatest.AsyncTestSuite

trait GlobalAsyncIOSpec extends AssertingSyntax with EffectTestSupport { asyncTestSuite: AsyncTestSuite =>

  implicit val ioRuntime: IORuntime = cats.effect.unsafe.implicits.global

}
