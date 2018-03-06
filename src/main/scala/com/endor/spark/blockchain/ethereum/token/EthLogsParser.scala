package com.endor.spark.blockchain.ethereum.token

import com.endor.spark.blockchain._
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.EthLog.LogObject

import scala.collection.mutable
import scala.collection.JavaConverters._

class EthLogsParser(web3j: Web3j) {
  private val knownBlockTimestamps = mutable.Map.empty[Long, Long].withDefault(blockNumber => {
    web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInt(blockNumber).bigInteger), false)
      .send()
      .getBlock
      .getTimestamp
      .longValue()
  })

  private def parseEthLog(log: LogObject): TokenTransferEvent = {
    val Seq(_, fromAddress, toAddress) = log.getTopics.asScala.map(_.drop(2)).map(_.bytes.drop(12))
    val blockNumber = log.getBlockNumber.longValue()
    TokenTransferEvent(
      log.getAddress.drop(2).bytes,
      fromAddress,
      toAddress,
      log.getData.drop(2).bytes,
      blockNumber,
      log.getTransactionHash.drop(2).bytes,
      log.getTransactionIndex.intValue(),
      knownBlockTimestamps(blockNumber)
    )
  }

  def fromEthLog(log: LogObject): Option[TokenTransferEvent] = {
    for {
      firstTopic <- log.getTopics.asScala.headOption
      result <- Option(firstTopic == TokenTransferEvent.topic && log.getTopics.asScala.lengthCompare(3) == 0)
        .filter(identity)
        .map(_ => parseEthLog(log))
    } yield result
  }
}
