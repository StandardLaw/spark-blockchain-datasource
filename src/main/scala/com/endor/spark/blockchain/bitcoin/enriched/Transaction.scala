package com.endor.spark.blockchain.bitcoin.enriched

import org.apache.spark.sql.{Encoder, Encoders}

final case class Transaction(blockHash: String, hash: String, isCoinBase: Boolean, time: Option[Long], sizeInBytes: Int,
                             fee: Option[Long])

object Transaction {
  implicit lazy val encoder: Encoder[Transaction] = Encoders.product[Transaction]
}

