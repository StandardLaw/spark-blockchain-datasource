package com.endor.spark.blockchain

import com.endor.spark.blockchain.ethereum.block.{EnrichedEthereumBlock, SimpleEthereumBlock}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrameReader, Dataset}

package object ethereum {
  implicit class EthereumDataFrameReader(reader: DataFrameReader) {
    def ethereum(path: String): Dataset[SimpleEthereumBlock] = reader
      .format("com.endor.spark.blockchain.ethereum.block")
      .load(path).as[SimpleEthereumBlock]

    def enrichedEthereum(path: String): Dataset[EnrichedEthereumBlock] = ethereum(path)
      .map((block: SimpleEthereumBlock) => block.toEnriched)
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
