package zio.sqs

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue

final case class SqsPublishEvent(
  body: String,
  attributes: Map[String, MessageAttributeValue],
  groupId: Option[String] = None,
  deduplicationId: Option[String] = None
)

object SqsPublishEvent {

  def apply(body: String): SqsPublishEvent = SqsPublishEvent()

}