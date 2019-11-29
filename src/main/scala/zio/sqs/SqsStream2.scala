package zio.sqs

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ DeleteMessageBatchRequest, ChangeMessageVisibilityBatchRequest, _ }
import zio.stream.{ Sink, Stream }
import zio.{ IO, Schedule, Task, ZIO }

import scala.jdk.CollectionConverters._

object SqsStream2 {

  trait Ack
  case object Delete           extends Ack
  case object Ignore           extends Ack
  case object ChangeVisibility extends Ack

  final case class MessageId(value: String)

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
      .fromEffect(Task.effect(requestBuilder))
      .forever
      .map(_.build())
      .mapMPar(settings.parallelism)(runReceiveMessageRequest(client, _))
      .takeWhile(_.nonEmpty || !settings.stopWhenQueueEmpty)
      .mapConcat(identity)
      .buffer(settings.parallelism * settings.maxNumberOfMessages)
  }

  def ack(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsAckSettings = SqsAckSettings()
  ) = {

    import ZioSyntax._

    val items
      : List[(Message, Ack)] = List.empty[(Message, Ack)] // e.g. IF we have this stream - connect 'apply' and 'ack'

    Stream
      .managed(
        Stream
          .fromIterable(items)
          .partition3({
            case (m, Delete)           => ZIO.succeed(Partition1[Message, Message, Message](m))
            case (m, Ignore)           => ZIO.succeed(Partition2[Message, Message, Message](m))
            case (m, ChangeVisibility) => ZIO.succeed(Partition3[Message, Message, Message](m))
            case (_, _)                => ZIO.fail[Throwable](new RuntimeException("invalid (message, action)"))
          })
      )
      .flatMap({
        case (deletes, ignores, changeVisibilities) =>
          val deleteResults = deletes
            .aggregateAsyncWithin(Sink.collectAllN[Message](settings.batchSize), Schedule.spaced(settings.duration))
            .map(buildDeleteRequest(queueUrl, _))
            .mapMPar(settings.parallelism)(runDeleteRequest(client, _))
            .mapConcat(identity)

          val ignoreResults = ignores.map(it => MessageId(it.messageId()))

          val changeVisibilityResults = changeVisibilities
            .aggregateAsyncWithin(Sink.collectAllN[Message](settings.batchSize), Schedule.spaced(settings.duration))
            .map(buildChangeVisibilityRequest(queueUrl, _))
            .mapMPar(settings.parallelism)(runChangeVisibilityRequest(client, _))
            .mapConcat(identity)

          deleteResults ++ ignoreResults ++ changeVisibilityResults
      })
  }

  private def buildDeleteRequest(queueUrl: String, ms: List[Message]): DeleteMessageBatchRequest = {
    val entries = ms.map(m =>
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

  private def buildChangeVisibilityRequest(queueUrl: String, ms: List[Message]): ChangeMessageVisibilityBatchRequest = {
    val entries = ms.map(m =>
      ChangeMessageVisibilityBatchRequestEntry
        .builder()
        .id(m.messageId())
        .receiptHandle(m.receiptHandle())
        .visibilityTimeout(30) // TODO: set in case class
        .build()
    )

    ChangeMessageVisibilityBatchRequest
      .builder()
      .queueUrl(queueUrl)
      .entries(entries.asJava)
      .build()
  }

  private def runReceiveMessageRequest(client: SqsAsyncClient, req: ReceiveMessageRequest): Task[List[Message]] =
    Task.effectAsync[List[Message]] { cb =>
      client
        .receiveMessage(req)
        .handle[Unit] { (result, err) =>
          err match {
            case null => cb(IO.succeed(result.messages.asScala.toList))
            case ex   => cb(IO.fail(ex))
          }
        }
      ()
    }

  private def runDeleteRequest(client: SqsAsyncClient, req: DeleteMessageBatchRequest): Task[List[MessageId]] =
    Task.effectAsync[List[MessageId]] { cb =>
      client
        .deleteMessageBatch(req)
        .handle[Unit] { (res, err) =>
          err match {
            case null =>
              res match {
                case rs if rs.failed().isEmpty =>
                  cb(IO.succeed(rs.successful().asScala.map(it => MessageId(it.id())).toList))
                case rs =>
                  cb(IO.fail(new RuntimeException("Failed to delete some messages.")))
              }
            case ex => cb(IO.fail(ex))
          }
        }
      ()
    }

  private def runChangeVisibilityRequest(
    client: SqsAsyncClient,
    req: ChangeMessageVisibilityBatchRequest
  ): Task[List[MessageId]] =
    Task.effectAsync[List[MessageId]] { cb =>
      client
        .changeMessageVisibilityBatch(req)
        .handle[Unit] { (res, err) =>
          err match {
            case null =>
              res match {
                case rs if rs.failed().isEmpty =>
                  cb(IO.succeed(rs.successful().asScala.map(it => MessageId(it.id())).toList))
                case rs =>
                  cb(IO.fail(new RuntimeException("Failed to change visibility for some messages.")))
              }
            case ex => cb(IO.fail(ex))
          }
        }
      ()
    }
}
