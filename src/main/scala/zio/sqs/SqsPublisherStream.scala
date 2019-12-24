package zio.sqs

import java.util.function.BiFunction

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResponse }
import zio.clock.Clock
import zio.stream.{ Sink, Stream, ZStream }
import zio.{ IO, Promise, Queue, Schedule, Task, ZManaged }

import scala.jdk.CollectionConverters._

object SqsPublisherStream {

  def producer(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsPublisherStreamSettings = SqsPublisherStreamSettings()
  ): ZManaged[Clock, Throwable, SqsProducer] = {
    val eventQueueSize = nextPower2(settings.batchSize * settings.parallelism)
    for {
      eventQueue  <- Queue.bounded[SqsPublishEvent](eventQueueSize).toManaged(_.shutdown)
      failedQueue <- Queue.bounded[SqsPublishEvent](eventQueueSize).toManaged(_.shutdown)
      stream = (ZStream
        .fromQueue(failedQueue)
        .merge(ZStream.fromQueue(eventQueue)))
        .aggregateAsyncWithin(
          Sink.collectAllN[SqsPublishEvent](settings.batchSize.toLong),
          Schedule.spaced(settings.duration)
        )
        .map(buildSendMessageBatchRequest(queueUrl, _))
        .mapMParUnordered(settings.parallelism)(it => runSendMessageBatchRequest(client, it._1, it._2))
        .mapConcat(identity)
      _ <- stream.runDrain.toManaged_.fork
    } yield new SqsProducer {
      override def produce(e: SqsPublishEvent): Task[SqsPublishErrorOrResult] =
        for {
          done <- Promise.make[Throwable, SqsPublishErrorOrResult]
          _    <- eventQueue.offer(e)
          response <- done.await
        } yield response

      override def produceBatch(es: List[SqsPublishEvent]): Task[List[SqsPublishErrorOrResult]] = ???

      override def sendStream: Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrResult] =
        ???
    }
  }

  def sendStream(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsPublisherStreamSettings = SqsPublisherStreamSettings()
  ): Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrResult] = { es =>
    es.aggregateAsyncWithin(
        Sink.collectAllN[SqsPublishEvent](settings.batchSize.toLong),
        Schedule.spaced(settings.duration)
      )
      .map(buildSendMessageBatchRequest(queueUrl, _))
      .mapMParUnordered(settings.parallelism)(it => runSendMessageBatchRequest(client, it._1, it._2))
      .mapConcat(identity)
  }

  private[sqs] def buildSendMessageBatchRequest(
    queueUrl: String,
    es: List[SqsPublishEvent]
  ): (SendMessageBatchRequest, List[(SqsPublishEvent, Int)]) = {
    val indexedMessages = es.zipWithIndex
    val entries = indexedMessages.map {
      case (m: SqsPublishEvent, id: Int) =>
        SendMessageBatchRequestEntry
          .builder()
          .id(id.toString)
          .messageBody(m.body)
          .messageAttributes(m.attributes.asJava)
          .messageGroupId(m.groupId.orNull)
          .messageDeduplicationId(m.deduplicationId.orNull)
          .build()
    }

    val req = SendMessageBatchRequest
      .builder()
      .queueUrl(queueUrl)
      .entries(entries.asJava)
      .build()

    (req, indexedMessages)
  }

  private[sqs] def runSendMessageBatchRequest(
    client: SqsAsyncClient,
    req: SendMessageBatchRequest,
    indexedMessages: List[(SqsPublishEvent, Int)]
  ): Task[List[SqsPublishErrorOrResult]] =
    Task.effectAsync[List[SqsPublishErrorOrResult]]({ cb =>
      client
        .sendMessageBatch(req)
        .handleAsync[Unit](new BiFunction[SendMessageBatchResponse, Throwable, Unit] {
          override def apply(res: SendMessageBatchResponse, err: Throwable): Unit =
            err match {
              case null =>
                val m = indexedMessages.map(it => (it._2.toString, it._1)).toMap

                val ss = res
                  .successful()
                  .asScala
                  .map(res => Right(SqsPublishEventResult(res, m(res.id()))): SqsPublishErrorOrResult)
                  .toList

                val es = res
                  .failed()
                  .asScala
                  .map(err => Left(SqsPublishEventError(err, m(err.id()))): SqsPublishErrorOrResult)
                  .toList

                cb(IO.succeed(ss ++ es))
              case ex =>
                cb(IO.fail(ex))
            }
        })
      ()
    })

  private[sqs] def nextPower2(n: Int): Int = {
    var m: Int = n
    m -= 1
    m |= m >> 1
    m |= m >> 2
    m |= m >> 4
    m |= m >> 8
    m |= m >> 16
    m += 1
    m
  }

  final case class AwaitableSqsPublishEvent(
    event: SqsPublishEvent,
    done: Promise[Throwable, SqsPublishErrorOrResult]
  )
}
