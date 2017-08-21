package com.itv.bucky

import scala.concurrent.{ExecutionContext, Future}

package object future {

  def futureMonad(implicit executionContext: ExecutionContext) = new MonadError[Future, Throwable] {
    override def apply[A](a: => A): Future[A] = Future(a)

    override def map[A, B](m: Future[A])(f: (A) => B): Future[B] = m.map(f)

    override def flatMap[A, B](m: Future[A])(f: (A) => Future[B]): Future[B] = m.flatMap(f)

    override def raiseError[A](e: Throwable): Future[A] = Future.failed(e)

    override def handleError[A](fa: Future[A])(f: (Throwable) => Future[A]): Future[A] = fa.recoverWith {
      case t: Throwable => f(t)
    }
  }

}
