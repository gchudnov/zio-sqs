package zio.sqs

import zio.Task
import zio.clock.Clock
import zio.stream.{Stream, ZSink, ZStream}

trait Producer {

  def produce(e: SqsPublishEvent): Task[SqsPublishErrorOrEvent]

  def produceBatch(es: List[SqsPublishEvent]): Task[List[SqsPublishErrorOrEvent]]

  def sendSink: ZSink[Any, Throwable, Nothing, List[SqsPublishEvent], List[SqsPublishErrorOrEvent]] =
    ZSink.await[List[SqsPublishErrorOrEvent]].contramapM[Any, Throwable, List[SqsPublishEvent]](produceBatch)

  def sendStream: Stream[Throwable, SqsPublishEvent] => ZStream[Clock, Throwable, SqsPublishErrorOrEvent]
}
