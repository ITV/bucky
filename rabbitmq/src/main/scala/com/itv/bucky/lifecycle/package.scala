package com.itv.bucky

import com.itv.lifecycle.{Lifecycle, NoOpLifecycle}

package object lifecycle {

  implicit val lifecycleMonad = new Monad[Lifecycle] {
    override def apply[A](a: => A): Lifecycle[A] = NoOpLifecycle(a)

    override def map[A, B](m: Lifecycle[A])(f: (A) => B): Lifecycle[B] = m.map(f)

    override def flatMap[A, B](m: Lifecycle[A])(f: (A) => Lifecycle[B]): Lifecycle[B] = m.flatMap(f)
  }

}
