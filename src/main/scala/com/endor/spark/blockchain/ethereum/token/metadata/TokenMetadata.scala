package com.endor.spark.blockchain.ethereum.token.metadata

import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenMetadata(address: String, symbol: Option[String],
                               totalSupply: Option[String], decimals: Option[Int]) {
  def mergeWith(other: TokenMetadata): TokenMetadata =
    TokenMetadata(address, symbol orElse other.symbol,
      totalSupply orElse other.totalSupply, decimals orElse other.decimals)
}

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]

  def fromConcrete(address: String, symbol: String, totalSupply: String, decimals: Int): TokenMetadata =
    new TokenMetadata(address, Option(symbol), Option(totalSupply), Option(decimals))
}
