package com.endor.spark.blockchain.bitcoin.transaction

import com.endor.spark.blockchain.bitcoin.Hash
import org.apache.spark.sql.{Encoder, Encoders}

final case class Transaction(blockHash: Hash, transactionHash: Hash, version: Long,
                             inputCount: Int, totalInputValue: Long, outputCount: Int, totalOutputValue: Long,
                             lockTime: Long, updateTime: Long, blockTime: Long, fee: Option[Long])

object Transaction {
  implicit lazy val encoder: Encoder[Transaction] = Encoders.product[Transaction]
}
