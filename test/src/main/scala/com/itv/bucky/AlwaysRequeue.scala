package com.itv.bucky

import scala.language.higherKinds

case class AlwaysRequeue[F[_], E](implicit F: MonadError[F, E]) extends RequeueHandler[F, String] {
  override def apply(message: String): F[RequeueConsumeAction] =
    F.apply(Requeue)
}
