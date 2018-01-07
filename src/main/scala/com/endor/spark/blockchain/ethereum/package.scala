package com.endor.spark.blockchain

import com.endor.spark.blockchain.ethereum.block.{EnrichedEthereumBlock, SimpleEthereumBlock}
import com.endor.spark.blockchain.ethereum.token.TokenTransferEvent
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrameReader, Dataset}

package object ethereum {
  implicit class EthereumDataFrameReader(reader: DataFrameReader) {
    def ethereum(path: String): Dataset[SimpleEthereumBlock] = reader
      .format("com.endor.spark.blockchain.ethereum.block")
      .load(path).as[SimpleEthereumBlock]

    def enrichedEthereum(path: String): Dataset[EnrichedEthereumBlock] = ethereum(path)
      .map((block: SimpleEthereumBlock) => block.toEnriched)

    def tokenTransferEvents(fromBlock: Long, toBlock: Long,
                            host: String, hosts: String*): Dataset[TokenTransferEvent] = {
      reader
        .options(Map(
          "fromBlock" -> fromBlock.toString,
          "toBlock" -> toBlock.toString,
          "hosts" -> (hosts :+ host).mkString(",")
        ))
        .format("com.endor.spark.blockchain.ethereum.token")
        .load()
        .as[TokenTransferEvent]
    }
  }

  implicit class EthereumDataStreamReader(reader: DataStreamReader) {
    def ethereum(path: String): Dataset[SimpleEthereumBlock] = reader
      .format("com.endor.spark.blockchain.ethereum.block")
      .load(path)
      .as[SimpleEthereumBlock]

    def enrichedEthereum(path: String): Dataset[EnrichedEthereumBlock] = ethereum(path)
      .map((block: SimpleEthereumBlock) => block.toEnriched)
  }
}
