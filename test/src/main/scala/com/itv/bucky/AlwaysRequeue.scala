package com.itv.bucky

import scala.language.higherKinds

object AlwaysRequeue {
  def apply[F[_], E](implicit F: MonadError[F, E]): RequeueHandler[F, String] = new RequeueHandler[F, String] {
    override def apply(message: String): F[RequeueConsumeAction] =
    F.apply(Requeue)
  }
}
