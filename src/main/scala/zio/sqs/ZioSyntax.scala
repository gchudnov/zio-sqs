package zio.sqs

import zio._
import zio.stream._

object ZioSyntax {

  implicit class RichZStream[-R, +E, +A](self: ZStream[R, E, A]) {

    /**
     * Split a stream by a predicate. The faster stream may advance by up to buffer elements further than the slower one.
     */
    final def partition3[B, C, R1 <: R, E1 >: E](
      p: A => ZIO[R1, E1, Either[B, C]],
      buffer: Int = 16
    ): ZManaged[R1, E1, (ZStream[Any, E1, B], ZStream[Any, E1, C])] =
      self
        .mapM(p)
        .distributedWith(2, buffer, {
          case Left(_)  => ZIO.succeed(_ == 0)
          case Right(_) => ZIO.succeed(_ == 1)
        })
        .flatMap {
          case q1 :: q2 :: Nil =>
            ZManaged.succeed {
              (
                ZStream.fromQueueWithShutdown(q1).unTake.collect { case Left(x)  => x },
                ZStream.fromQueueWithShutdown(q2).unTake.collect { case Right(x) => x }
              )
            }
          case _ => ZManaged.dieMessage("Internal error.")
        }
  }

}
