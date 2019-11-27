package zio.sqs

import zio._
import zio.stream._

object ZioSyntax {

  sealed abstract class Partition[+A, +B, +C]()     extends Product with Serializable
  final case class Partition1[+A, +B, +C](value: A) extends Partition[A, B, C]
  final case class Partition2[+A, +B, +C](value: B) extends Partition[A, B, C]
  final case class Partition3[+A, +B, +C](value: C) extends Partition[A, B, C]

  implicit class RichZStream[-R, +E, +A](self: ZStream[R, E, A]) {

    /**
     * Split a stream by a predicate. The faster stream may advance by up to buffer elements further than the slower one.
     */
    final def partition3[B, C, D, R1 <: R, E1 >: E](
      p: A => ZIO[R1, E1, Partition[B, C, D]],
      buffer: Int = 24
    ): ZManaged[R1, E1, (ZStream[Any, E1, B], ZStream[Any, E1, C], ZStream[Any, E1, D])] =
      self
        .mapM(p)
        .distributedWith(3, buffer, {
          case Partition1(_) => ZIO.succeed(_ == 0)
          case Partition2(_) => ZIO.succeed(_ == 1)
          case Partition3(_) => ZIO.succeed(_ == 2)
        })
        .flatMap {
          case q1 :: q2 :: q3 :: Nil =>
            ZManaged.succeed {
              (
                ZStream.fromQueueWithShutdown(q1).unTake.collect { case Partition1(x) => x },
                ZStream.fromQueueWithShutdown(q2).unTake.collect { case Partition2(x) => x },
                ZStream.fromQueueWithShutdown(q3).unTake.collect { case Partition3(x) => x }
              )
            }
          case _ => ZManaged.dieMessage("Internal error.")
        }
  }

}
