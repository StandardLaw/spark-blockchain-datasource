package com.endor.spark.blockchain.bitcoin.io

import com.endor.spark.blockchain.bitcoin.Hash
import org.apache.spark.sql.{Encoder, Encoders}

final case class IO(blockHash: Hash, transactionHash: Hash, address: Option[String], value: Option[Long], isInput: Boolean, index: Long, blockTimeMs: Long)

object IO {
  implicit lazy val encoder: Encoder[IO] = Encoders.product[IO]
}
