package com.endor.spark.blockchain.ethereum.token

import com.endor.spark.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import org.web3j.protocol.core.methods.response.EthLog.LogObject
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.JavaConverters._

final case class TokenTransferEvent(contractAddress: Array[Byte], fromAddress: Array[Byte], toAddress: Array[Byte],
                                    value: Array[Byte], blockNumber: Long, transactionHash: Array[Byte],
                                    transactionIndex: Int)

object TokenTransferEvent {
  val topic: String = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
  implicit lazy val encoder: Encoder[TokenTransferEvent] = Encoders.product[TokenTransferEvent]
  implicit lazy val format: OFormat[TokenTransferEvent] = {
    val reads = (
      (__ \ "contractAddress").read[String].map(_.bytes) and
        (__ \ "fromAddress").read[String].map(_.bytes) and
        (__ \ "toAddress").read[String].map(_.bytes) and
        (__ \ "value").read[String].map(_.bytes) and
        (__ \ "blockNumber").read[Long] and
        (__ \ "transactionHash").read[String].map(_.bytes) and
        (__ \ "transactionIndex").read[Int]
      )(TokenTransferEvent.apply _)

    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    val writes: OWrites[TokenTransferEvent] = (o: TokenTransferEvent) =>  JsObject(Map(
        "contractAddress" -> JsString(o.contractAddress.hex),
        "fromAddress" -> JsString(o.fromAddress.hex),
        "toAddress" -> JsString(o.toAddress.hex),
        "value" -> JsString(o.value.hex),
        "blockNumber" -> JsNumber(o.blockNumber),
        "transactionHash" -> JsString(o.transactionHash.hex),
        "transactionIndex" -> JsNumber(o.transactionIndex)
      ))
    OFormat(reads, writes)
  }

  private def parseEthLog(log: LogObject): TokenTransferEvent = {
    val Seq(_, fromAddress, toAddress) = log.getTopics.asScala.map(_.drop(2)).map(_.bytes.drop(12))
    TokenTransferEvent(
      log.getAddress.drop(2).bytes,
      fromAddress,
      toAddress,
      log.getData.drop(2).bytes,
      log.getBlockNumber.longValue(),
      log.getTransactionHash.drop(2).bytes,
      log.getTransactionIndex.intValue()
    )
  }

  def fromEthLog(log: LogObject): Option[TokenTransferEvent] = {
    for {
      firstTopic <- log.getTopics.asScala.headOption
      result <- if(firstTopic == topic && log.getTopics.asScala.lengthCompare(3) == 0) Option(parseEthLog(log)) else None
    } yield result
  }
}
