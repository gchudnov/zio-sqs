package zio.sqs

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue
import zio.duration._
import zio.sqs.SqsPublishStreamSpecUtil._
import zio.sqs.SqsPublisherStream.SqsRequestEntry
import zio.sqs.ZioSqsMockServer._
import zio.stream.{Sink, Stream}
import zio.test.Assertion._
import zio.test._
import zio._

import scala.jdk.CollectionConverters._

object SqsPublishStreamSpec
    extends DefaultRunnableSpec(
      suite("SqsPublishStream")(
        testM("buildSendMessageBatchRequest creates a new request") {
          val queueUrl = "sqs://queue"

          val attr = MessageAttributeValue
            .builder()
            .dataType("String")
            .stringValue("Bob")
            .build()

          val events = List(
            SqsPublishEvent(
              body = "A",
              attributes = Map("Name" -> attr),
              groupId = Some("g1"),
              deduplicationId = Some("d1")
            ),
            SqsPublishEvent(
              body = "B",
              attributes = Map.empty[String, MessageAttributeValue],
              groupId = Some("g2"),
              deduplicationId = Some("d2")
            )
          )

          for {
            reqEntries <- ZIO.traverse(events) { event =>
              for {
                done <- Promise.make[Throwable, SqsPublishErrorOrResult]
              } yield SqsRequestEntry(event, done, 0)
            }
          } yield {
            val req = SqsPublisherStream.buildSendMessageBatchRequest(queueUrl, reqEntries)

            val innerReq = req.inner
            val innerReqEntries             = req.inner.entries().asScala

            assert(req.entries, equalTo(reqEntries)) &&
            assert(innerReq.hasEntries, equalTo(true)) &&
            assert(innerReqEntries.size, equalTo(2)) &&
            assert(innerReqEntries(0).id(), equalTo("0")) &&
            assert(innerReqEntries(0).messageBody(), equalTo("A")) &&
            assert(innerReqEntries(0).messageAttributes().size(), equalTo(1)) &&
            assert(innerReqEntries(0).messageAttributes().asScala.contains("Name"), equalTo(true)) &&
            assert(innerReqEntries(0).messageAttributes().asScala("Name"), equalTo(attr)) &&
            assert(Option(innerReqEntries(0).messageGroupId()), equalTo(Some("g1"))) &&
            assert(Option(innerReqEntries(0).messageDeduplicationId()), equalTo(Some("d1"))) &&
            assert(innerReqEntries(1).id(), equalTo("1")) &&
            assert(innerReqEntries(1).messageBody(), equalTo("B")) &&
            assert(innerReqEntries(1).messageAttributes().size(), equalTo(0)) &&
            assert(Option(innerReqEntries(1).messageGroupId()), equalTo(Some("g2"))) &&
            assert(Option(innerReqEntries(1).messageDeduplicationId()), equalTo(Some("d2")))
          }
        },
        testM("runSendMessageBatchRequest can be executed") {
          val queueName = "SqsPublishStreamSpec_runSend"
          for {
            events <- Util
                       .stringGen(10)
                       .sample
                       .map(_.value.map(SqsPublishEvent(_)))
                       .run(Sink.await[List[SqsPublishEvent]])
            server <- serverResource
            client <- clientResource
            retryQueue <- queueResource(1)
            dones <- server.use { _ =>
              client.use { c =>
                retryQueue.use { q =>
                  for {
                    _ <- SqsUtils.createQueue(c, queueName)
                    queueUrl <- SqsUtils.getQueueUrl(c, queueName)
                    reqEntries <- ZIO.traverse(events) { event =>
                      for {
                        done <- Promise.make[Throwable, SqsPublishErrorOrResult]
                      } yield SqsRequestEntry(event, done, 0)
                    }
                    req = SqsPublisherStream.buildSendMessageBatchRequest(queueUrl, reqEntries)
                    retryDelay = 1.millisecond
                    retryCount = 1
                    reqSender = SqsPublisherStream.runSendMessageBatchRequest(c, q, retryDelay, retryCount) _
                    _ <- reqSender(req)
                  } yield ZIO.traverse(reqEntries)(entry => entry.done.await)
                }
              }
            }
            isAllRight <- dones.map(_.forall(_.isRight))
          } yield {
            assert(isAllRight, equalTo(true))
          }
        },
        testM("events can be published using a stream") {
          val queueName = "SqsPublishStreamSpec_sendStream"
          for {
            events <- Util
                       .stringGen(23)
                       .sample
                       .map(_.value.map(SqsPublishEvent(_)))
                       .run(Sink.await[List[SqsPublishEvent]])
            server <- serverResource
            client <- clientResource
            results <- server.use { _ =>
              client.use { c =>
                  for {
                    _ <- SqsUtils.createQueue(c, queueName)
                    queueUrl <- SqsUtils.getQueueUrl(c, queueName)
                    producer <- Task.succeed(SqsPublisherStream.producer(c, queueUrl))
                    results <- producer.use { p =>
                      p.sendStream(Stream(events: _*)).run(Sink.collectAll[SqsPublishErrorOrResult])  // replace with .via when ZIO > RC17 is released
                    }
                  } yield results
                }
              }
          } yield {
            assert(results.size, equalTo(events.size)) &&
            assert(results.forall(_.isRight), equalTo(true))
          }
        }
      ),
      List(TestAspect.executionStrategy(ExecutionStrategy.Sequential))
    )

object SqsPublishStreamSpecUtil {

  def queueResource(capacity: Int): Task[ZManaged[Any, Throwable, Queue[SqsRequestEntry]]] = Task.succeed {
    Queue.bounded[SqsRequestEntry](capacity).toManaged(_.shutdown)
  }

}
