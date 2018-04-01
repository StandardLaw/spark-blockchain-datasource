package com.endor.spark.blockchain.ethereum.token.streaming

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum.EthereumClient
import com.endor.spark.blockchain.ethereum.token.TokenTransferEvent
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ethereum.core._
import org.ethereum.facade.Ethereum

import scala.collection.JavaConverters._

class TokenStreamSource(databaseLocation: String, syncEnabled: Boolean)(@transient val sparkSession: SparkSession)
  extends Source with Serializable {

  @transient private lazy val ethereum: Ethereum = EthereumClient.getClient(databaseLocation, syncEnabled)
  @transient private lazy val blockchain: BlockchainImpl = ethereum.getBlockchain match {
    case bc: BlockchainImpl => bc
  }

  override def schema: StructType = TokenTransferEvent.encoder.schema

  override def getOffset: Option[Offset] = {
    Option(blockchain.getBestBlock)
      .map(_.getHeader.getNumber)
      .map(LongOffset.apply)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val calculatedDf = for {
      firstBlock <- start.flatMap(LongOffset.convert) orElse Option(LongOffset(0))
      lastBlock <- LongOffset.convert(end)
    } yield {
      val transactionStore = blockchain.getTransactionStore
      val events = for {
        block <- (firstBlock.offset until lastBlock.offset).map(blockchain.getBlockByNumber)
        tx <- block.getTransactionsList.asScala
        txInfo = transactionStore.get(tx.getHash, block.getHash)
        logInfo <- txInfo.getReceipt.getLogInfoList.asScala
        _ <- logInfo.getTopics.asScala.headOption.map(_.getData.hex).filter(_ == TokenTransferEvent.topic.drop(2))
        fromAddress <- logInfo.getTopics.asScala.drop(1).headOption.map(_.getData)
        toAddress <- logInfo.getTopics.asScala.drop(2).headOption.map(_.getData)
      } yield TokenTransferEvent(
        logInfo.getAddress,
        fromAddress,
        toAddress,
        logInfo.getData,
        block.getNumber,
        tx.getHash,
        txInfo.getIndex,
        block.getTimestamp
      )
      sparkSession.createDataset(events)
    }
    calculatedDf.getOrElse(sparkSession.emptyDataset[TokenTransferEvent]).toDF()
  }

  override def stop(): Unit = {
    ethereum.close()
  }
}
