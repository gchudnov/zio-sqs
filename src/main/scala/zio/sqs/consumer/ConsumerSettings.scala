package zio.sqs.consumer

/**
 * Settings for the consumer
 */
final case class ConsumerSettings(
  batchSize: Int = 10,
  parallelism: Int = 16
)
