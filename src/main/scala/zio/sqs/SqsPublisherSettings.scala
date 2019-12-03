package zio.sqs

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue
import zio.duration._

case class SqsPublisherSettings(
  delaySeconds: Option[Int] = None,
  messageAttributes: Map[String, MessageAttributeValue] = Map(),
  messageDeduplicationId: String = "",
  messageGroupId: String = "",
  batchSize: Long = 10L,
  duration: Duration = 1.second,
  parallelism: Int = 16
)
