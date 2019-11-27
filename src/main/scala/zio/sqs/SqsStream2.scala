package zio.sqs

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ DeleteMessageBatchRequest, _ }
import zio.stream.{ Sink, Stream }
import zio.{ IO, Schedule, Task, ZIO }

import scala.jdk.CollectionConverters._

object SqsStream2 {

  trait Ack
  case object Delete           extends Ack
  case object Ignore           extends Ack
  case object ChangeVisibility extends Ack

  def apply(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsStreamSettings = SqsStreamSettings()
  ): Stream[Throwable, Message] = {

    val requestBuilder = ReceiveMessageRequest.builder
      .queueUrl(queueUrl)
      .attributeNamesWithStrings(settings.attributeNames.asJava)
      .messageAttributeNames(settings.messageAttributeNames.asJava)
      .maxNumberOfMessages(settings.maxNumberOfMessages)
      .visibilityTimeout(settings.visibilityTimeout)
      .waitTimeSeconds(settings.waitTimeSeconds)

    Stream
      .fromEffect({
        Task.effect(requestBuilder)
      })
      .forever
      .map(_.build())
      .mapMPar(settings.parallelism)(req => {
        Task.effectAsync[List[Message]] { cb =>
          client
            .receiveMessage(req)
            .handle[Unit]((result, err) => {
              err match {
                case null => cb(IO.succeed(result.messages.asScala.toList))
                case ex   => cb(IO.fail(ex))
              }
            })
          ()
        }
      })
      .takeWhile(_.nonEmpty || !settings.stopWhenQueueEmpty)
      .mapConcat(identity)
      .buffer(settings.parallelism * settings.maxNumberOfMessages)
      .mapM(msg => IO.when(settings.autoDelete)(deleteMessage(client, queueUrl, msg)).as(msg))
  }

  def ack(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsAckSettings = SqsAckSettings()
  ): Stream[Throwable, (Message, Ack)] = {

    import ZioSyntax._

    val items: List[(Message, Ack)] = List.empty[(Message, Ack)] // e.g. IF we have this stream....

    Stream
      .fromIterable(items)
      .partition3({
        case (m, Delete)           => ZIO.succeed(Partition1[Message, Message, Message](m))
        case (m, Ignore)           => ZIO.succeed(Partition2[Message, Message, Message](m))
        case (m, ChangeVisibility) => ZIO.succeed(Partition3[Message, Message, Message](m))
      })
      .flatMap({
        case (deletes, ignores, changeVisibilities) =>
          deletes
            .aggregateAsyncWithin(Sink.collectAllN(settings.batchSize), Schedule.spaced(settings.duration))
            .map(buildDeleteRequest(queueUrl, _))
            .mapMPar(settings.parallelism)(req => {
              Task.effectAsync[List[Message]] { cb =>
                client
                  .deleteMessageBatch(req)
                  .handle[Unit]((res, err) => {
                    err match {
                      case ex => cb(IO.fail(ex))
                      case null =>
                        res match {
                          case rs if rs.failed().isEmpty =>
                            cb(IO.succeed(rs.successful().asScala.toList))
                          case rs =>
                            cb(IO.fail(new RuntimeException("Failed to delete some messages.")))
                        }
                    }
                  })
              }
            })

      })
    // (m, a) -> [ split in 3 streams -> commit each stream separately -> join streams and have one stream as an output ]
    // [] -- implement as a single operator

  }

  private def buildDeleteRequest(queueUrl: String, ms: List[Message]): DeleteMessageBatchRequest = {
    val entries = ms.map(
      m =>
        DeleteMessageBatchRequestEntry
          .builder()
          .id(m.messageId())
          .receiptHandle(m.receiptHandle())
          .build()
    )

    DeleteMessageBatchRequest
      .builder()
      .queueUrl(queueUrl)
      .entries(entries.asJava)
      .build()
  }

  def deleteMessage(client: SqsAsyncClient, queueUrl: String, msg: Message): Task[Unit] =
    Task.effectAsync[Unit] { cb =>
      client
        .deleteMessage(
          DeleteMessageRequest
            .builder()
            .queueUrl(queueUrl)
            .receiptHandle(msg.receiptHandle())
            .build()
        )
        .handle[Unit]((_, err) => {
          err match {
            case null => cb(IO.unit)
            case ex   => cb(IO.fail(ex))
          }
        })
      ()
    }
}
