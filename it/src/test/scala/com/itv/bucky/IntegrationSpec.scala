package com.itv.bucky

import cats.effect.unsafe.{IORuntime, IORuntimeConfig, Scheduler}

import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor}
import scala.concurrent.ExecutionContext

trait IntegrationSpec {

  val schedulerExecutor = new ScheduledThreadPoolExecutor(
    1,
    { r =>
      val t = new Thread(r)
      t.setName("s")
      t.setDaemon(true)
      t.setPriority(Thread.MAX_PRIORITY)
      t
    }
  )
  schedulerExecutor.setRemoveOnCancelPolicy(true)
  val scheduler: Scheduler = Scheduler.fromScheduledExecutor(schedulerExecutor)
  val packageIORuntime: IORuntime = IORuntime.apply(
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300)),
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(300)),
    scheduler,
    () => (),
    IORuntimeConfig()
  )

}
