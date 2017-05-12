package com.itv.bucky.task

import scalaz.\/


object TaskExt {

  type TaskResult = \/[Throwable, Unit]

}
