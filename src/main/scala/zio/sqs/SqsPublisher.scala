package zio.sqs

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import zio.clock.Clock
import zio.sqs.SqsStream2.MessageId
import zio.stream.{Sink, Stream, ZStream}
import zio.{IO, Schedule, Task}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.jdk.CollectionConverters._

object SqsPublisher {

  trait Event {
    def body: String
    def attributes: Map[String, MessageAttributeValue]
    def groupId: String
    def deduplicationId: String
  }

  final case class SimpleEvent(body: String) extends Event {
    override def attributes: Map[String, MessageAttributeValue] = Map.empty[String, MessageAttributeValue]
    override def groupId: String = null
    override def deduplicationId: String = null
  }

  def send(
    client: SqsAsyncClient,
    queueUrl: String,
    msg: String,
    settings: SqsPublisherSettings = SqsPublisherSettings()
  ): Task[Unit] =
    Task.effectAsync[Unit] { cb =>
      client.sendMessage {
        val b1 = SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody(msg)
          .messageAttributes(settings.messageAttributes.asJava)
        val b2 = if (settings.messageGroupId.nonEmpty) b1.messageGroupId(settings.messageGroupId) else b1
        val b3 =
          if (settings.messageDeduplicationId.nonEmpty) b2.messageDeduplicationId(settings.messageDeduplicationId)
          else b2
        val b4 = settings.delaySeconds.fold(b3)(b3.delaySeconds(_))
        b4.build
      }.handle[Unit] { (_, err) =>
        err match {
          case null => cb(IO.unit)
          case ex   => cb(IO.fail(ex))
        }
      }
      ()
    }

  def sendStream(
    client: SqsAsyncClient,
    queueUrl: String,
    settings: SqsPublisherSettings
  )(ms: Stream[Throwable, Event]): ZStream[Clock, Throwable, MessageId] = {
    val ec = buildSendExecutionContext(settings.parallelism)
    ms.aggregateAsyncWithin(Sink.collectAllN[Event](settings.batchSize), Schedule.spaced(settings.duration))
      .map(buildSendMessageBatchRequest(queueUrl, _))
      .mapMParUnordered(settings.parallelism)(runSendMessageBatchRequest(client, ec, _))
      .mapConcat(identity)
  }

  private def buildSendMessageBatchRequest(queueUrl: String, ms: List[Event]): SendMessageBatchRequest = {
    val entries = ms.zipWithIndex.map {
      case (m: Event, id: Int) =>
        SendMessageBatchRequestEntry
          .builder()
          .id(id.toString)
          .messageBody(m.body)
          .messageAttributes(m.attributes.asJava)
          .messageGroupId(m.groupId)
          .messageDeduplicationId(m.deduplicationId)
          .build()
    }

    SendMessageBatchRequest
      .builder()
      .queueUrl(queueUrl)
      .entries(entries.asJava)
      .build()
  }

  def runSendMessageBatchRequest(client: SqsAsyncClient, ec: ExecutionContext, req: SendMessageBatchRequest): Task[List[MessageId]] =
    Task.effectAsync[List[MessageId]]({ cb =>
      client
        .sendMessageBatch(req)
        .handle[Unit] { (res, err) =>
          err match {
            case null =>
              res match {
                case rs if rs.failed().isEmpty =>
                  cb(IO.succeed(rs.successful().asScala.map(it => MessageId(it.id())).toList))
                case rs =>
                  cb(IO.fail(new RuntimeException("Failed to publish some messages.")))
              }
            case ex => cb(IO.fail(ex))
          }
        }
      ()
    }).on(ec)

  def buildSendExecutionContext(parallelism: Int): ExecutionContextExecutor = ExecutionContext.fromExecutor(

    scala.concurrent.ExecutionContext.global
    new java.util.concurrent.ForkJoinPool(parallelism)
  )

}
