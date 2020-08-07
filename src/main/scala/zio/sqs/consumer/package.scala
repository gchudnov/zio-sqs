package zio.sqs

package object consumer {

  /**
   * Specifies the result of acknowledging en event.
   * The result either an error [[zio.sqs.consumer.AcknowledgeError]] or the event itself [[zio.sqs.consumer.ConsumerEvent]]
   * @tparam T type of the event to publish
   */
  type ErrorOrEvent[T] = Either[AcknowledgeError[T], ConsumerEvent[T]]

}
