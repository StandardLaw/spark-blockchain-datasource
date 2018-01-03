package com.endor.spark.blockchain

import com.endor.spark.blockchain.bitcoin.transactions.Transaction
import org.apache.spark.sql.{DataFrameReader, Dataset}

package object bitcoin {
  type Hash = Array[Byte]

  implicit class RawBitcoinDataFrameReader(reader: DataFrameReader) {
    def bitcoin(implicit network: Network = Network.Main): { def transactions(path: String): Dataset[Transaction] } = new {
      def transactions(path: String): Dataset[Transaction] = {
        reader
          .option("network", network.id)
          .format("com.endor.spark.blockchain.bitcoin.transactions")
          .load(path)
          .as[Transaction]
      }
    }
  }
}
