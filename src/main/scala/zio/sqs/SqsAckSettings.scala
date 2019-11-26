package zio.sqs

import zio._
import zio.duration._

case class SqsAckSettings(
  attributeNames: List[String] = Nil,
  maxNumberOfMessages: Int = 1,
  messageAttributeNames: List[String] = Nil,
  visibilityTimeout: Int = 30,
  waitTimeSeconds: Int = 20,
  autoDelete: Boolean = true,
  stopWhenQueueEmpty: Boolean = false,
  parallelism: Int = 16
)
