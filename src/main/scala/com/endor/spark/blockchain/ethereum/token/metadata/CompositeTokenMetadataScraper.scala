package com.endor.spark.blockchain.ethereum.token.metadata

import scala.concurrent.{ExecutionContext, Future}

class CompositeTokenMetadataScraper(underlyingScraper: TokenMetadataScraper, underlyingScrapers: TokenMetadataScraper*)
  extends TokenMetadataScraper {
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  override def scrapeAddress(address: String)(implicit ec: ExecutionContext): Future[TokenMetadata] =
    (Seq(underlyingScraper) ++ underlyingScrapers).map(_.scrapeAddress(address)).reduce {
      (a, b) => a recoverWith {
        case _ => b
      }
    }
}
