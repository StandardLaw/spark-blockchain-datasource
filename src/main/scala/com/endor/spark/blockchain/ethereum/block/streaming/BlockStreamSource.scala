package com.endor.spark.blockchain.ethereum.block.streaming

import com.endor.spark.blockchain.ethereum.EthereumClient
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ethereum.core.BlockchainImpl
import org.ethereum.facade.Ethereum

class BlockStreamSource(databaseLocation: String, syncEnabled: Boolean)
                       (@transient val sparkSession: SparkSession) extends Source with Serializable {

  @transient private lazy val ethereum: Ethereum = EthereumClient.getClient(databaseLocation, syncEnabled)
  @transient private lazy val blockchain: BlockchainImpl = ethereum.getBlockchain match {
    case bc: BlockchainImpl => bc
  }

  override def schema: StructType = SimpleEthereumBlock.encoder.schema

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
      val blocks = (firstBlock.offset until lastBlock.offset)
        .map(blockchain.getBlockByNumber)
        .map(SimpleEthereumBlock.fromEthereumjBlock)
      sparkSession.createDataset(blocks)
    }
    calculatedDf.getOrElse(sparkSession.emptyDataset[SimpleEthereumBlock]).toDF()
  }

  override def stop(): Unit = {
    ethereum.close()
  }
}
