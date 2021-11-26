package com.itv.bucky.test

import cats.effect.IO
import cats.effect.testing.RuntimePlatform
import cats.effect.unsafe.IORuntime

import org.scalactic.source.Position
import org.scalatest.AsyncTestSuite
import org.scalatest.enablers.Retrying
import org.scalatest.time.Span
import cats.effect.testing.scalatest.AssertingSyntax
import cats.effect.testing.scalatest.EffectTestSupport

trait GlobalAsyncIOSpec extends AssertingSyntax with EffectTestSupport { asyncTestSuite: AsyncTestSuite =>

  implicit val ioRuntime: IORuntime = cats.effect.unsafe.implicits.global

}
