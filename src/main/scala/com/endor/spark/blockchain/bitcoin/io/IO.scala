package com.endor.spark.blockchain.bitcoin.io

import com.endor.spark.blockchain.bitcoin.Hash
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}

final case class IO(blockHash: Hash, transactionHash: Hash, address: Option[String], value: Option[Long],
                    isInput: Boolean, index: Long, timeMs: Option[Long])

object IO {
  implicit lazy val encoder: Encoder[IO] = Encoders.product[IO]
}

private[io] final case class RawInput(blockHash: Hash, transactionHash: Hash, time: Option[Long],
                                      fromAddress: Option[String], index: Long,
                                      outputTransaction: Hash, outputIndex: Long,
                                      joinExpr: String)

private[io] object RawInput {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit lazy val encoder: ExpressionEncoder[RawInput] =
    Encoders.product[RawInput].asInstanceOf[ExpressionEncoder[RawInput]]
}

private[io] final case class RawOutput(blockHash: Hash, transactionHash: Hash, time: Option[Long],
                                       toAddress: Option[String], index: Long, value: Option[Long],
                                       joinExpr: String)

private[io] object RawOutput {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit lazy val encoder: ExpressionEncoder[RawOutput] =
    Encoders.product[RawOutput].asInstanceOf[ExpressionEncoder[RawOutput]]
}

