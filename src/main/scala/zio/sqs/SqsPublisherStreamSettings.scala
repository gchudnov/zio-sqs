package zio.sqs

import zio.duration.{ Duration, _ }

final case class SqsPublisherStreamSettings(
  batchSize: Int = 10,
  duration: Duration = 500.millisecond,
  parallelism: Int = 16,
  retryDelay: Duration = 250.millisecond,
  retryMaxCount: Int = 10
)
