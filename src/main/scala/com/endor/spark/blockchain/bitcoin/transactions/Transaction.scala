package com.endor.spark.blockchain.bitcoin.transactions

import com.endor.spark.blockchain.bitcoin._
import org.apache.spark.sql.{Encoder, Encoders}

final case class Input(fromAddress: Option[String], index: Long, outputTransaction: Hash, outputIndex: Long)

object Input {
  implicit lazy val encoder: Encoder[Input] = Encoders.product[Input]
}

final case class Output(toAddress: Option[String], index: Long, value: Option[Long])

object Output {
  implicit lazy val encoder: Encoder[Output] = Encoders.product[Output]
}

final case class Transaction(blockHash: Hash, hash: Hash, isCoinBase: Boolean,
                             inputs: Seq[Input], outputs: Seq[Output], time: Option[Long], sizeInBytes: Int)

object Transaction {
  implicit lazy val encoder: Encoder[Transaction] = Encoders.product[Transaction]
}
