package com.endor.spark.blockchain.ethereum.token.metadata

import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenMetadata(address: String, name: Option[String], symbol: Option[String],
                               totalSupply: Option[String], decimals: Option[Int]) {
  def mergeWith(other: TokenMetadata): TokenMetadata =
    TokenMetadata(address,
      name.filter(!_.isEmpty) orElse other.name.filter(!_.isEmpty),
      symbol.filter(!_.isEmpty) orElse other.symbol.filter(!_.isEmpty),
      totalSupply.filter(!_.isEmpty) orElse other.totalSupply.filter(!_.isEmpty),
      decimals orElse other.decimals)

  def isComplete: Boolean = {
    val allNonEmptyAndDefined = for {
      n <- name
      s <- symbol
      t <- totalSupply
      _ <- decimals
    } yield !n.isEmpty && !s.isEmpty && !t.isEmpty

    allNonEmptyAndDefined.getOrElse(false)
  }
}

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]

  def apply(address: String, name: String, symbol: String, totalSupply: String, decimals: Option[Int]): TokenMetadata =
    new TokenMetadata(address, Option(name).filter(!_.isEmpty),
      Option(symbol).filter(!_.isEmpty), Option(totalSupply).filter(!_.isEmpty), decimals)

  def fromConcrete(address: String, name: String, symbol: String,
                   totalSupply: String, decimals: Option[Int]): TokenMetadata =
    apply(address, name, symbol, totalSupply, decimals)
}
