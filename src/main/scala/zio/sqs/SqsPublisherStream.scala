package zio.sqs

import java.util.function.BiFunction

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResponse }
import zio.clock.Clock
import zio.stream.{ Sink, Stream, ZStream }
import zio.{ IO, Promise, Queue, Schedule, Task, ZIO, ZManaged }

import scala.jdk.CollectionConverters._

object SqsPublisherStream {

  def producer(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsPublisherStreamSettings = SqsPublisherStreamSettings()
  ): ZManaged[Clock, Throwable, SqsProducer] = {
    val eventQueueSize = nextPower2(settings.batchSize * settings.parallelism)
    for {
      eventQueue  <- Queue.bounded[SqsRequestEntry](eventQueueSize).toManaged(_.shutdown)
      failedQueue <- Queue.bounded[SqsRequestEntry](eventQueueSize).toManaged(_.shutdown)
      stream = (ZStream
        .fromQueue(failedQueue)
        .merge(ZStream.fromQueue(eventQueue)))
        .aggregateAsyncWithin(
          Sink.collectAllN[SqsRequestEntry](settings.batchSize.toLong),
          Schedule.spaced(settings.duration)
        )
        .map(buildSendMessageBatchRequest(queueUrl, _))
        .mapMParUnordered(settings.parallelism)(req => runSendMessageBatchRequest(client, req))
        .mapConcat(identity)
      _ <- stream.runDrain.toManaged_.fork
    } yield new SqsProducer {

      override def produce(e: SqsPublishEvent): Task[SqsPublishErrorOrResult] =
        for {
          done     <- Promise.make[Throwable, SqsPublishErrorOrResult]
          _        <- eventQueue.offer(SqsRequestEntry(e, done))
          response <- done.await
        } yield response

      override def produceBatch(es: Iterable[SqsPublishEvent]): Task[List[SqsPublishErrorOrResult]] =
        ZIO
          .traverse(es) { e =>
            for {
              done <- Promise.make[Throwable, SqsPublishErrorOrResult]
            } yield SqsRequestEntry(e, done)
          }
          .flatMap(es => eventQueue.offerAll(es) *> ZIO.collectAllPar(es.map(_.done.await)))

      override def sendStream: Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrResult] =
        ???
    }
  }

//  def sendStream(
//    client: SqsAsyncClient,
//    queueUrl: String,
//    settings: SqsPublisherStreamSettings = SqsPublisherStreamSettings()
//  ): Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrResult] = { es =>
//    es.aggregateAsyncWithin(
//        Sink.collectAllN[SqsPublishEvent](settings.batchSize.toLong),
//        Schedule.spaced(settings.duration)
//      )
//      .map(buildSendMessageBatchRequest(queueUrl, _))
//      .mapMParUnordered(settings.parallelism)(it => runSendMessageBatchRequest(client, it._1, it._2))
//      .mapConcat(identity)
//  }

  private[sqs] def buildSendMessageBatchRequest(queueUrl: String, entries: List[SqsRequestEntry]): SqsRequest = {
    val reqEntries = entries.zipWithIndex.map {
      case (e: SqsRequestEntry, index: Int) =>
        SendMessageBatchRequestEntry
          .builder()
          .id(index.toString)
          .messageBody(e.event.body)
          .messageAttributes(e.event.attributes.asJava)
          .messageGroupId(e.event.groupId.orNull)
          .messageDeduplicationId(e.event.deduplicationId.orNull)
          .build()
    }

    val req = SendMessageBatchRequest
      .builder()
      .queueUrl(queueUrl)
      .entries(reqEntries.asJava)
      .build()

    SqsRequest(req, entries)
  }

  private[sqs] def runSendMessageBatchRequest(client: SqsAsyncClient, req: SqsRequest): Task[List[SqsPublishErrorOrResult]] =
    Task.effectAsync[List[SqsPublishErrorOrResult]]({ cb =>
      client
        .sendMessageBatch(req.inner)
        .handleAsync[Unit](new BiFunction[SendMessageBatchResponse, Throwable, Unit] {
          override def apply(res: SendMessageBatchResponse, err: Throwable): Unit =
            err match {
              case null =>
                val m = req.entries.zipWithIndex.map(it => (it._2.toString, it._1.event)).toMap

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

  private[sqs] final case class SqsRequestEntry(
    event: SqsPublishEvent,
    done: Promise[Throwable, SqsPublishErrorOrResult]
  )

  private[sqs] final case class SqsRequest(
    inner: SendMessageBatchRequest,
    entries: List[SqsRequestEntry]
  )
}
