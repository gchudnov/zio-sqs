package zio

package object sqs {

  type SqsPublishErrorOrResult = Either[SqsPublishEventError, SqsPublishEventResult]

}
