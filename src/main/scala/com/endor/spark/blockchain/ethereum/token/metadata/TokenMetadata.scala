package com.endor.spark.blockchain.ethereum.token.metadata

import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenMetadata(address: String, symbol: Option[String],
                               totalSupply: Option[String], decimals: Option[Int]) {
  def mergeWith(other: TokenMetadata): TokenMetadata =
    TokenMetadata(address,
      symbol.filter(!_.isEmpty) orElse other.symbol.filter(!_.isEmpty),
      totalSupply.filter(!_.isEmpty) orElse other.totalSupply.filter(!_.isEmpty),
      decimals orElse other.decimals)

  def isComplete: Boolean = {
    val allNonEmptyAndDefined = for {
      s <- symbol
      t <- totalSupply
      _ <- decimals
    } yield !s.isEmpty && !t.isEmpty

    allNonEmptyAndDefined.getOrElse(false)
  }
}

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]

  def fromConcrete(address: String, symbol: String, totalSupply: String, decimals: Int): TokenMetadata =
    new TokenMetadata(address, Option(symbol), Option(totalSupply), Option(decimals))
}
