package com.liorregev.spark.blockchain.ethereum.token

import com.liorregev.spark.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import org.web3j.protocol.core.methods.response.EthLog.LogObject

import scala.collection.JavaConverters._

final case class TokenTransferEvent(contractAddress: Array[Byte], fromAddress: Array[Byte], toAddress: Array[Byte],
                                    value: Double, blockNumber: Long, transactionHash: Array[Byte],
                                    transactionIndex: Int)

object TokenTransferEvent {
  val topic: String = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
  implicit val encoder: Encoder[TokenTransferEvent] = Encoders.product[TokenTransferEvent]

  private def parseEth1Log(log: LogObject): TokenTransferEvent = {
    val Seq(_, fromAddress, toAddress) = log.getTopics.asScala.map(_.drop(2)).map(_.bytes.drop(12))
    TokenTransferEvent(
      log.getAddress.drop(2).bytes,
      fromAddress,
      toAddress,
      BigDecimal(BigInt(log.getData.drop(2).bytes)).doubleValue(),
      log.getBlockNumber.longValue(),
      log.getTransactionHash.drop(2).bytes,
      log.getTransactionIndex.intValue()
    )
  }

  def fromEthLog(log: LogObject): Option[TokenTransferEvent] = {
    for {
      firstTopic <- log.getTopics.asScala.headOption
      result <- if(firstTopic == topic) Option(parseEth1Log(log)) else None
    } yield result
  }
}
