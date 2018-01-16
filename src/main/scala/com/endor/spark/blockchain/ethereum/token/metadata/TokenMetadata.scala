package com.endor.spark.blockchain.ethereum.token.metadata

import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenMetadata(address: String, symbol: Option[String],
                               totalSupply: Option[String], decimals: Option[Int]) {
  def mergeWith(other: TokenMetadata): TokenMetadata =
    TokenMetadata(address,
      symbol.filter(_.length > 0) orElse other.symbol.filter(_.length > 0),
      totalSupply.filter(_.length > 0) orElse other.totalSupply.filter(_.length > 0),
      decimals orElse other.decimals)

  def isComplete: Boolean =
    Seq(symbol, totalSupply, decimals).forall(_.isDefined) &&
      Seq(symbol, totalSupply).flatMap(_.map(_.length > 0)).forall(identity)
}

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]

  def fromConcrete(address: String, symbol: String, totalSupply: String, decimals: Int): TokenMetadata =
    new TokenMetadata(address, Option(symbol), Option(totalSupply), Option(decimals))
}
