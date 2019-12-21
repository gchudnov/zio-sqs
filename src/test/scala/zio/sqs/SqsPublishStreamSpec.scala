package zio.sqs

import software.amazon.awssdk.services.sqs.model.{BatchResultErrorEntry, MessageAttributeValue}
import zio.test.Assertion._
import zio.test._

import scala.jdk.CollectionConverters._

object SqsPublishStreamSpec
  extends DefaultRunnableSpec(
    suite("SqsPublishStream")(
      test("buildSendMessageBatchRequest can be created") {
        val queueUrl = "sqs://queue"

        val attr = MessageAttributeValue
          .builder()
          .dataType("String")
          .stringValue("Bob")
          .build()

        val events = List(
          SqsPublishEvent(body = "A", attributes = Map("Name" -> attr), groupId = Some("g1"), deduplicationId = Some("d1")),
          SqsPublishEvent(body = "B", attributes = Map.empty[String, MessageAttributeValue], groupId = Some("g2"), deduplicationId = Some("d2"))
        )

        val (req, indexedMessages) = SqsPublisherStream.buildSendMessageBatchRequest(queueUrl, events)
        val reqEntries = req.entries().asScala

        assert(reqEntries.size, equalTo(2))

        assert(reqEntries(0).id(), equalTo("0"))
        assert(reqEntries(0).messageBody(), equalTo("A"))
        assert(reqEntries(0).messageAttributes().size(), equalTo(1))
        assert(reqEntries(0).messageAttributes().asScala.contains("Name"), equalTo(true))
        assert(reqEntries(0).messageAttributes().asScala("Name"), equalTo(attr))
        assert(Option(reqEntries(0).messageGroupId()), equalTo(Some("g1")))
        assert(Option(reqEntries(0).messageDeduplicationId()), equalTo(Some("d1")))

        assert(reqEntries(1).id(), equalTo("1"))
        assert(reqEntries(1).messageBody(), equalTo("B"))
        assert(reqEntries(1).messageAttributes().size(), equalTo(0))
        assert(Option(reqEntries(1).messageGroupId()), equalTo(Some("g2")))
        assert(Option(reqEntries(1).messageDeduplicationId()), equalTo(Some("d2")))

        assert(indexedMessages, equalTo(events.zipWithIndex))
      },
      test("buildSendMessageBatchRequest can be created from strings") {
        val queueUrl = "sqs://queue"
        val events = List(
          SqsPublishEvent("a"),
          SqsPublishEvent("b")
        )

        val (req, indexedMessages) = SqsPublisherStream.buildSendMessageBatchRequest(queueUrl, events)
        val reqEntries = req.entries().asScala

        assert(reqEntries.size, equalTo(2))

        assert(reqEntries(0).id(), equalTo("0"))
        assert(reqEntries(0).messageBody(), equalTo("a"))
        assert(reqEntries(0).messageAttributes().size(), equalTo(0))
        assert(Option(reqEntries(0).messageGroupId()), equalTo(None))
        assert(Option(reqEntries(0).messageDeduplicationId()), equalTo(None))

        assert(reqEntries(1).id(), equalTo("1"))
        assert(reqEntries(1).messageBody(), equalTo("b"))
        assert(reqEntries(1).messageAttributes().size(), equalTo(0))
        assert(Option(reqEntries(1).messageGroupId()), equalTo(None))
        assert(Option(reqEntries(1).messageDeduplicationId()), equalTo(None))

        assert(indexedMessages, equalTo(events.zipWithIndex))
      }
    ),
    List(TestAspect.executionStrategy(ExecutionStrategy.Sequential))
  )
