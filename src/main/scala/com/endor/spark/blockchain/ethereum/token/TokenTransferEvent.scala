package com.endor.spark.blockchain.ethereum.token

import com.endor.spark.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import play.api.libs.functional.syntax._
import play.api.libs.json._



final case class TokenTransferEvent(contractAddress: Array[Byte], fromAddress: Array[Byte], toAddress: Array[Byte],
                                    value: Array[Byte], blockNumber: Long, transactionHash: Array[Byte],
                                    transactionIndex: Int, timestamp: Long)

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
        (__ \ "transactionIndex").read[Int] and
        (__ \ "timestamp").read[Long]
      )(TokenTransferEvent.apply _)

    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    val writes: OWrites[TokenTransferEvent] = (o: TokenTransferEvent) =>  JsObject(Map(
        "contractAddress" -> JsString(o.contractAddress.hex),
        "fromAddress" -> JsString(o.fromAddress.hex),
        "toAddress" -> JsString(o.toAddress.hex),
        "value" -> JsString(o.value.hex),
        "blockNumber" -> JsNumber(o.blockNumber),
        "transactionHash" -> JsString(o.transactionHash.hex),
        "transactionIndex" -> JsNumber(o.transactionIndex),
        "timestamp" -> JsNumber(o.timestamp)
      ))
    OFormat(reads, writes)
  }
}
