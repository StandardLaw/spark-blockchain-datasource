package com.endor.spark.blockchain.ethereum.token.metadata

import scala.concurrent.{ExecutionContext, Future}

class CompositeTokenMetadataScraper(underlyingScrapers: TokenMetadataScraper*) extends TokenMetadataScraper {
  override def scrapeAddress(address: String)(implicit ec: ExecutionContext): Future[TokenMetadata] =
    underlyingScrapers.map(_.scrapeAddress(address)).reduce {
      (a, b) => a recoverWith {
        case _ => b
      }
    }
}
