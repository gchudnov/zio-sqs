package zio.sqs

import software.amazon.awssdk.services.sqs.model.{
  BatchResultErrorEntry,
  MessageAttributeValue,
  SendMessageBatchRequest,
  SendMessageBatchRequestEntry,
  SendMessageBatchResponse,
  SendMessageBatchResultEntry
}
import zio.{ Promise, Queue, Task, UIO, ZIO, ZManaged }
import zio.duration._
import zio.sqs.SqsPublishStreamSpecUtil._
import zio.sqs.SqsPublisherStream.{ SqsRequest, SqsRequestEntry, SqsResponseErrorEntry }
import zio.sqs.ZioSqsMockServer._
import zio.stream.{ Sink, Stream }
import zio.test.Assertion._
import zio.test._

import scala.jdk.CollectionConverters._

object SqsPublishStreamSpec
    extends DefaultRunnableSpec(
      suite("SqsPublishStream")(
        test("nextPower2 can be calculated") {
          assert(SqsPublisherStream.nextPower2(0), equalTo(0)) &&
          assert(SqsPublisherStream.nextPower2(1), equalTo(1)) &&
          assert(SqsPublisherStream.nextPower2(2), equalTo(2)) &&
          assert(SqsPublisherStream.nextPower2(9), equalTo(16)) &&
          assert(SqsPublisherStream.nextPower2(129), equalTo(256))
        },
        testM("SqsRequestEntry can be created") {
          val attr = MessageAttributeValue
            .builder()
            .dataType("String")
            .stringValue("Bob")
            .build()

          val pe = SqsPublishEvent(
            body = "A",
            attributes = Map("Name" -> attr),
            groupId = Some("g1"),
            deduplicationId = Some("d1")
          )

          for {
            done         <- Promise.make[Throwable, SqsPublishErrorOrResult]
            requestEntry = SqsRequestEntry(pe, done, 10)
            isDone       <- requestEntry.done.isDone
          } yield {
            assert(requestEntry.event, equalTo(pe)) &&
            assert(isDone, equalTo(false)) &&
            assert(requestEntry.retryCount, equalTo(10))
          }
        },
        testM("SqsRequest can be created") {
          val attr = MessageAttributeValue
            .builder()
            .dataType("String")
            .stringValue("Bob")
            .build()

          val pe = SqsPublishEvent(
            body = "A",
            attributes = Map("Name" -> attr),
            groupId = Some("g1"),
            deduplicationId = Some("d1")
          )

          val batchRequestEntry = SendMessageBatchRequestEntry
            .builder()
            .id("1")
            .messageBody("{}")
            .build()

          val batchReq = SendMessageBatchRequest
            .builder()
            .queueUrl("queueUrl")
            .entries(List(batchRequestEntry).asJava)
            .build()

          for {
            done         <- Promise.make[Throwable, SqsPublishErrorOrResult]
            requestEntry = SqsRequestEntry(pe, done, 10)
            request      = SqsRequest(batchReq, List(requestEntry))
          } yield {
            assert(request.inner, equalTo(batchReq)) &&
            assert(request.entries, equalTo(List(requestEntry)))
          }
        },
        testM("SqsResponseErrorEntry can be created") {
          val event = SqsPublishEvent("e1")
          val errEntry =
            BatchResultErrorEntry.builder().id("id1").code("code2").message("message3").senderFault(true).build()

          val eventError = SqsPublishEventError(errEntry, event)

          for {
            done     <- Promise.make[Throwable, SqsPublishErrorOrResult]
            errEntry = SqsResponseErrorEntry(done, eventError)
            isDone   <- errEntry.done.isDone
          } yield {
            assert(errEntry.error, equalTo(eventError)) &&
            assert(isDone, equalTo(false))
          }
        },
        testM("SendMessageBatchResponse can be partitioned") {
          val retryMaxCount = 10
          val rs            = Range(0, 4).toList
          val ids           = rs.map(_.toString)
          val bodies        = rs.map(_ + 'A').map(_.toChar.toString)
          val retries       = List(1, 2, retryMaxCount, 3)
          for {
            dones          <- ZIO.traverse(Range(0, 4))(_ => Promise.make[Throwable, SqsPublishErrorOrResult])
            requestEntries = bodies.zip(dones).zip(retries).map { case ((a, b), c) => SqsRequestEntry(SqsPublishEvent(a), b, c) }
            m              = ids.zip(requestEntries).toMap
            resultEntry0   = SendMessageBatchResultEntry.builder().id("0").build()
            errorEntry1    = BatchResultErrorEntry.builder().id("1").code("ServiceUnavailable").senderFault(false).build()
            errorEntry2    = BatchResultErrorEntry.builder().id("2").code("ThrottlingException").senderFault(false).build()
            errorEntry3    = BatchResultErrorEntry.builder().id("3").code("AccessDeniedException").senderFault(false).build()
            res = SendMessageBatchResponse
              .builder()
              .successful(resultEntry0)
              .failed(errorEntry1, errorEntry2, errorEntry3)
              .build()
            partitioner                     = SqsPublisherStream.partitionResponse(m, retryMaxCount) _
            (successful, retryable, errors) = partitioner(res)
          } yield {
            assert(successful.toList.size, equalTo(1)) &&
            assert(retryable.toList.size, equalTo(1)) &&
            assert(errors.toList.size, equalTo(2))
          }
        },
        testM("SendMessageBatchResponse can be partitioned and mapped") {
          val retryMaxCount = 10
          val rs            = Range(0, 4).toList
          val ids           = rs.map(_.toString)
          val bodies        = rs.map(_ + 'A').map(_.toChar.toString)
          val retries       = List(1, 2, retryMaxCount, 3)
          for {
            dones          <- ZIO.traverse(Range(0, 4))(_ => Promise.make[Throwable, SqsPublishErrorOrResult])
            requestEntries = bodies.zip(dones).zip(retries).map { case ((a, b), c) => SqsRequestEntry(SqsPublishEvent(a), b, c) }
            m              = ids.zip(requestEntries).toMap
            resultEntry0   = SendMessageBatchResultEntry.builder().id("0").build()
            errorEntry1    = BatchResultErrorEntry.builder().id("1").code("ServiceUnavailable").senderFault(false).build()
            errorEntry2    = BatchResultErrorEntry.builder().id("2").code("ThrottlingException").senderFault(false).build()
            errorEntry3    = BatchResultErrorEntry.builder().id("3").code("AccessDeniedException").senderFault(false).build()
            res = SendMessageBatchResponse
              .builder()
              .successful(resultEntry0)
              .failed(errorEntry1, errorEntry2, errorEntry3)
              .build()
            partitioner                                          = SqsPublisherStream.partitionResponse(m, retryMaxCount) _
            (successful, retryable, errors)                      = partitioner(res)
            mapper                                               = SqsPublisherStream.mapResponse(m) _
            (successfulEntries, retryableEntries, errorsEntries) = mapper(successful, retryable, errors)
          } yield {
            assert(successful.toList.size, equalTo(1)) &&
            assert(retryable.toList.size, equalTo(1)) &&
            assert(errors.toList.size, equalTo(2)) &&
            assert(successfulEntries.toList.size, equalTo(1)) &&
            assert(retryableEntries.toList.size, equalTo(1)) &&
            assert(errorsEntries.toList.size, equalTo(2)) &&
            assert(successfulEntries.toList.map(_.event.body), hasSameElements(List("A"))) &&
            assert(retryableEntries.toList.map(_.event.body), hasSameElements(List("B"))) &&
            assert(errorsEntries.toList.map(_.error.event.body), hasSameElements(List("C", "D")))
          }
        },
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

            val innerReq        = req.inner
            val innerReqEntries = req.inner.entries().asScala

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
        }
//        testM("runSendMessageBatchRequest can be executed") {
//          val queueName = "SqsPublishStreamSpec_runSend"
//          for {
//            events <- Util
//                       .stringGen(10)
//                       .sample
//                       .map(_.value.map(SqsPublishEvent(_)))
//                       .run(Sink.await[List[SqsPublishEvent]])
//            server <- serverResource
//            client <- clientResource
//            retryQueue <- queueResource(1)
//            dones <- server.use { _ =>
//              client.use { c =>
//                retryQueue.use { q =>
//                  for {
//                    _ <- SqsUtils.createQueue(c, queueName)
//                    queueUrl <- SqsUtils.getQueueUrl(c, queueName)
//                    reqEntries <- ZIO.traverse(events) { event =>
//                      for {
//                        done <- Promise.make[Throwable, SqsPublishErrorOrResult]
//                      } yield SqsRequestEntry(event, done, 0)
//                    }
//                    req = SqsPublisherStream.buildSendMessageBatchRequest(queueUrl, reqEntries)
//                    retryDelay = 1.millisecond
//                    retryCount = 1
//                    reqSender = SqsPublisherStream.runSendMessageBatchRequest(c, q, retryDelay, retryCount) _
//                    _ <- reqSender(req)
//                  } yield ZIO.traverse(reqEntries)(entry => entry.done.await)
//                }
//              }
//            }
//            isAllRight <- dones.map(_.forall(_.isRight))
//          } yield {
//            assert(isAllRight, equalTo(true))
//          }
//        },
//        testM("events can be published using a stream") {
//          val queueName = "SqsPublishStreamSpec_sendStream"
//          for {
//            events <- Util
//                       .stringGen(23)
//                       .sample
//                       .map(_.value.map(SqsPublishEvent(_)))
//                       .run(Sink.await[List[SqsPublishEvent]])
//            server <- serverResource
//            client <- clientResource
//            results <- server.use { _ =>
//              client.use { c =>
//                  for {
//                    _ <- SqsUtils.createQueue(c, queueName)
//                    queueUrl <- SqsUtils.getQueueUrl(c, queueName)
//                    producer <- Task.succeed(SqsPublisherStream.producer(c, queueUrl))
//                    results <- producer.use { p =>
//                      p.sendStream(Stream(events: _*)).run(Sink.collectAll[SqsPublishErrorOrResult])  // replace with .via when ZIO > RC17 is released
//                    }
//                  } yield results
//                }
//              }
//          } yield {
//            assert(results.size, equalTo(events.size)) &&
//            assert(results.forall(_.isRight), equalTo(true))
//          }
//        }
      ),
      List(TestAspect.executionStrategy(ExecutionStrategy.Sequential))
    )

object SqsPublishStreamSpecUtil {

  def queueResource(capacity: Int): Task[ZManaged[Any, Throwable, Queue[SqsRequestEntry]]] = Task.succeed {
    Queue.bounded[SqsRequestEntry](capacity).toManaged(_.shutdown)
  }

}
