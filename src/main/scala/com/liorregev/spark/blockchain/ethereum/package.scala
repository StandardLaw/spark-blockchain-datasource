package com.liorregev.spark.blockchain

import com.liorregev.spark.blockchain.ethereum.model.{EnrichedEthereumBlock, EthereumBlock}
import org.apache.spark.sql.{DataFrameReader, Dataset}

package object ethereum {
  /**
    * Adds a method, `ethereum`, to DataFrameReader that allows you to read avro files using
    * the DataFileReade
    */
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def ethereum: String => Dataset[EthereumBlock] = path => reader.option("enrich", "false")
      .format("com.liorregev.spark.blockchain.ethereum").load(path).as[EthereumBlock]

    def enrichedEthereum: String => Dataset[EnrichedEthereumBlock] = path => reader.option("enrich", "true")
      .format("com.liorregev.spark.blockchain.ethereum").load(path).as[EnrichedEthereumBlock]
  }
}
