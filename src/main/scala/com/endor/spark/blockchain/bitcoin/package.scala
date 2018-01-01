package com.endor.spark.blockchain

import com.endor.spark.blockchain.bitcoin.io.IO
import com.endor.spark.blockchain.bitcoin.transaction._
import org.apache.spark.sql.{DataFrameReader, Dataset}
import org.bitcoinj.core.NetworkParameters

package object bitcoin {
  type Hash = Array[Byte]
  
  implicit class BitcoinDataFrameReader(reader: DataFrameReader) {
    def bitcoin(path: String, id: String = NetworkParameters.PAYMENT_PROTOCOL_ID_MAINNET): Dataset[Transaction] = { // TODO: CoProduct instead of String
      reader
//      .option("enrich", "false")
        .option("network", id)
        .format("com.endor.spark.blockchain.bitcoin.transaction")
        .load(path)
        .as[Transaction]
    }

    def bitcoinIO(path: String, id: String = NetworkParameters.PAYMENT_PROTOCOL_ID_MAINNET): Dataset[IO] = {
      reader
//      .option("enrich", "false")
        .option("network", id)
        .format("com.endor.spark.blockchain.bitcoin.io")
        .load(path)
        .as[IO]
    }
  }
}
