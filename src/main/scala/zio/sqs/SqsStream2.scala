package zio.sqs

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import zio.stream.Stream
import zio.{IO, Task}

import scala.jdk.CollectionConverters._

object SqsStream2 {

  trait Ack
  case object Ignore extends Ack
  case object Delete extends Ack

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

    Stream.fromEffect({
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

  def ack(client: SqsAsyncClient,
          queueUrl: String,
          settings: SqsAckSettings = SqsAckSettings()): Stream[Throwable, (Message, Ack)] = {
    ???
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
