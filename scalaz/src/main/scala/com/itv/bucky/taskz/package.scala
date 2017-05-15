package com.itv.bucky

import scalaz.concurrent.Task

package object taskz {

  implicit val taskMonadError = new MonadError[Task, Throwable] {
    override def raiseError[A](e: Throwable): Task[A] = Task.fail(e)

    override def handleError[A](fa: Task[A])(f: (Throwable) => Task[A]): Task[A] = fa.handleWith { case e => f(e) }

    override def apply[A](a: => A): Task[A] = Task.apply(a)

    override def map[A, B](m: Task[A])(f: (A) => B): Task[B] = m.map(f)

    override def flatMap[A, B](m: Task[A])(f: (A) => Task[B]): Task[B] = m.flatMap(f)
  }

}
