package zio.sqs

import zio._
import zio.duration._

case class SqsAckSettings(
  batchSize: Int,
  duration: Duration
)
