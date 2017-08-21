package com.itv.bucky

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.util.Try

object TryUtil {

  def sequence[A, M[X] <: scala.TraversableOnce[X]](in: M[Try[A]])(
      implicit cbf: CanBuildFrom[M[Try[A]], A, M[A]]): Try[M[A]] =
    in.foldLeft(Try(cbf(in))) { (fr, fa) =>
        for (r <- fr; a <- fa) yield (r += a)
      }
      .map(_.result())

}
