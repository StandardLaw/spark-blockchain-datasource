package com.liorregev.spark.blockchain

import com.liorregev.spark.blockchain.ethereum.block.{EnrichedEthereumBlock, SimpleEthereumBlock}
import com.liorregev.spark.blockchain.ethereum.token.TokenTransferEvent
import org.apache.spark.sql.{DataFrameReader, Dataset}

package object ethereum {
  implicit class EthereumDataFrameReader(reader: DataFrameReader) {
    def ethereum(path: String): Dataset[SimpleEthereumBlock] = reader.option("enrich", "false")
      .format("com.liorregev.spark.blockchain.ethereum.block").load(path).as[SimpleEthereumBlock]

    def enrichedEthereum(path: String): Dataset[EnrichedEthereumBlock] = reader.option("enrich", "true")
      .format("com.liorregev.spark.blockchain.ethereum.block").load(path).as[EnrichedEthereumBlock]

    def tokenTransferEvents(fromBlock: Long, toBlock: Long,
                            host: String, hosts: String*): Dataset[TokenTransferEvent] = {
      reader
        .options(Map(
          "fromBlock" -> fromBlock.toString,
          "toBlock" -> toBlock.toString,
          "hosts" -> (hosts :+ host).mkString(",")
        ))
        .format("com.liorregev.spark.blockchain.ethereum.token")
        .load()
        .as[TokenTransferEvent]
    }
  }
}
