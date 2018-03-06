package com.endor.spark.blockchain.ethereum.token.metadata

import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenMetadata(address: String, name: Option[String], symbol: Option[String],
                               totalSupply: Option[String], decimals: Option[Int]) {
  def mergeWith(other: TokenMetadata): TokenMetadata =
    TokenMetadata(address,
      name.filter(_.nonEmpty) orElse other.name.filter(_.nonEmpty),
      symbol.filter(_.nonEmpty) orElse other.symbol.filter(_.nonEmpty),
      totalSupply.filter(_.nonEmpty) orElse other.totalSupply.filter(_.nonEmpty),
      decimals orElse other.decimals)

  def isComplete: Boolean = {
    val allNonEmptyAndDefined = for {
      n <- name
      s <- symbol
      t <- totalSupply
      _ <- decimals
    } yield n.nonEmpty && s.nonEmpty && t.nonEmpty

    allNonEmptyAndDefined.getOrElse(false)
  }
}

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]

  def apply(address: String, name: String, symbol: String, totalSupply: String, decimals: Option[Int]): TokenMetadata =
    new TokenMetadata(address, Option(name).filter(_.nonEmpty),
      Option(symbol).filter(_.nonEmpty), Option(totalSupply).filter(_.nonEmpty), decimals)

  def fromConcrete(address: String, name: String, symbol: String,
                   totalSupply: String, decimals: Option[Int]): TokenMetadata =
    apply(address, name, symbol, totalSupply, decimals)
}
