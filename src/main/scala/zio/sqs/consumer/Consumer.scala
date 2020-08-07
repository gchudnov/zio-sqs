package zio.sqs.consumer

import zio.{Promise, Queue, Task, ZManaged}
import zio.clock.Clock
import zio.sqs.producer.Producer.{nextPower2}
import zio.sqs.producer.{ErrorOrEvent, ProducerEvent}
import zio.stream.{Stream, ZSink, ZStream}

/**
 * Consumer that can be used to consume an event of type T from an SQS queue and acknowledge these events.
 * @tparam T type of the event to consume.
 */
trait Consumer[T] {

  /**
   * A stream of events from the SQS-queue.
   * @return stream with the events.
   *         Consumed events should be acknowledged within the provided `visibilityTimeout` or they will reappear in the SQS queue.
   */
  def stream: Stream[Throwable, ConsumerEvent[T]]

  /**
   * Acknowledges the provided event on SQS-server.
   * @param e event to acknowledge.
   * @return result of the operation.
   *         Fails if the served failed to acknowledge the message.
   */
  def ack(e: ConsumerEvent[T]): Task[ConsumerEvent[T]]

  /**
   * Acknowledges the provided events on SQS-server.
   * @param es events to acknowledge.
   * @return result of the operation.
   */
  def ackBatch(es: Iterable[ConsumerEvent[T]]): Task[List[ConsumerEvent[T]]]

  /**
   * Acknowledges the proded stream of events.
   * @return The stream of acknowledged events.
   */
  def ackStream: Stream[Throwable, ConsumerEvent[T]] => ZStream[Clock, Throwable, ConsumerEvent[T]]

  /**
   * The sink that can be used to acknowledge events.
   * @return Sink to acknowledge events
   */
  def ackSink: ZSink[Any, Throwable, Nothing, Iterable[ConsumerEvent[T]], Unit]
}

object Consumer {

  /**
   * Instantiates a new consumer.
   * @tparam R zio environment.
   * @tparam T type of the events to consume.
   * @return managed consumer for streaming events.
   */
  def make[R, T](
                  settings: ConsumerSettings
                ): ZManaged[R, Throwable, Consumer[T]] = {
    val eventQueueSize = nextPower2(settings.batchSize * settings.parallelism)
    for {
      eventQueue <- Queue.bounded[SqsRequestEntry[T]](eventQueueSize).toManaged(_.shutdown)
    }
  }


  /**
   * Request entry with bookkeeping information alongside of the received event.
   * @param event event to be published
   * @param done promise that tracks publishing completion.
   * @tparam T type of the event to publish.
   */
  private[sqs] final case class SqsRequestEntry[T](
                                                    event: ConsumerEvent[T],
                                                    done: Promise[Throwable, ErrorOrEvent[T]]
                                                  )
}
