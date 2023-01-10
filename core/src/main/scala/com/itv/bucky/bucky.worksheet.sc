import cats.effect.kernel.Resource
import cats.data.Kleisli
import cats.~>

import scala.language.higherKinds

trait In

trait Out

trait Trait[F[_]] { self =>
  type Handler[X[_], Y] = Y => X[Out]

  def something(): F[Unit]

  def problem(handler: Handler[F, In]): Resource[F, Unit]

  def mapK[G[_]](fk: F ~> G)(gk: G ~> F): Trait[G] =
    new Trait[G] {
      def something(): G[Unit] = fk(self.something())

      def problem(handler: Handler[G, In]): Resource[G, Unit] =  {
        val inner = Kleisli(handler).mapK(gk).run
        self.problem(inner).mapK(fk)
      }


    }
}
