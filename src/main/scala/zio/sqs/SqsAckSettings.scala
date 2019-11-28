package zio.sqs

import zio.duration._

case class SqsAckSettings(
  batchSize: Long = 10L,
  duration: Duration = 1.second,
  parallelism: Int = 16
)
