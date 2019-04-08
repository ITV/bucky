package com.itv.bucky.kamonSupport

import java.util.concurrent.LinkedBlockingQueue

import cats.effect.IO
import com.typesafe.config.Config
import kamon.{Kamon, SpanReporter}
import kamon.testkit.{Reconfigure, TestSpanReporter}
import kamon.trace.Span
import kamon.trace.Span.FinishedSpan
import kamon.util.Registration

import scala.collection.mutable.ListBuffer
import scala.util.Try

trait SpanSupport extends Reconfigure {

  class AccTestSpanReporter() extends SpanReporter {
    private val reportedSpans: ListBuffer[FinishedSpan] = ListBuffer[FinishedSpan]()

    override def reportSpans(spans: Seq[Span.FinishedSpan]): Unit = reportedSpans.synchronized(reportedSpans ++= spans.toList)
    def clear(): Unit                                             = reportedSpans.clear()
    override def start(): Unit                                    = {}
    override def stop(): Unit                                     = reportedSpans.clear()
    override def reconfigure(config: Config): Unit                = {}
    def spans: List[FinishedSpan]                                 = reportedSpans.synchronized(reportedSpans.toList)
  }

  def withSpanReporter(test: AccTestSpanReporter => Unit) = {
    val reporter = new AccTestSpanReporter()
    enableFastSpanFlushing()
    sampleAlways()
    applyConfig("kamon.trace.sampler = always")
    val registration = Kamon.addReporter(reporter)
    val result       = Try { test(reporter) }
    registration.cancel()
    result.get
  }
}
