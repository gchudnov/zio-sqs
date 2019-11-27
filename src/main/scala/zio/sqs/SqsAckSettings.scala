package zio.sqs

import zio._
import zio.duration._

case class SqsAckSettings(
  batchSize: Int = 10,
  duration: Duration = 1.second
)
