package zio.sqs

import zio.Task
import zio.clock.Clock
import zio.stream.{ Stream, ZSink, ZStream }

trait SqsProducer {

  def produce(e: SqsPublishEvent): Task[SqsPublishErrorOrResult]

  def produceBatch(es: Iterable[SqsPublishEvent]): Task[List[SqsPublishErrorOrResult]]

  def sendSink: ZSink[Any, Throwable, Nothing, List[SqsPublishEvent], List[SqsPublishErrorOrResult]] =
    ZSink.await[List[SqsPublishErrorOrResult]].contramapM[Any, Throwable, List[SqsPublishEvent]](produceBatch)

  def sendStream: Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrResult]
}
