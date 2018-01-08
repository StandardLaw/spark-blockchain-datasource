package com.endor.spark.blockchain.bitcoin.enriched

import com.endor.spark.blockchain.bitcoin.Hash
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}

final case class IO(transactionHash: String, address: Option[String], value: Option[Long],
                    isInput: Boolean, index: Long, timeMs: Option[Long])

object IO {
  implicit lazy val encoder: Encoder[IO] = Encoders.product[IO]
}

private[enriched] final case class RawInput(transactionHash: Hash, time: Option[Long],
                                            fromAddress: Option[String], index: Long,
                                            outputTransaction: Hash, outputIndex: Long,
                                            joinExpr: String)

private[enriched] object RawInput {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit lazy val encoder: ExpressionEncoder[RawInput] =
    Encoders.product[RawInput].asInstanceOf[ExpressionEncoder[RawInput]]
}

private[enriched] final case class RawOutput(transactionHash: Hash, time: Option[Long],
                                             toAddress: Option[String], index: Long, value: Option[Long],
                                             joinExpr: String)

private[enriched] object RawOutput {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit lazy val encoder: ExpressionEncoder[RawOutput] =
    Encoders.product[RawOutput].asInstanceOf[ExpressionEncoder[RawOutput]]
}

private[enriched] final case class TransactionFee(hash: Hash, fee: Long)

object TransactionFee {
  implicit lazy val encoder: Encoder[TransactionFee] = Encoders.product[TransactionFee]
}