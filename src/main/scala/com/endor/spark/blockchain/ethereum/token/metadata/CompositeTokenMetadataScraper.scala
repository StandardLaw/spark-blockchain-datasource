package com.endor.spark.blockchain.ethereum.token.metadata

import scala.concurrent.{ExecutionContext, Future}


class CompositeTokenMetadataScraper(underlyingScraper: TokenMetadataScraper, underlyingScrapers: TokenMetadataScraper*)
  extends TokenMetadataScraper {
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  override def scrapeAddress(address: String)(implicit ec: ExecutionContext): Future[TokenMetadata] =
    underlyingScrapers.foldLeft(underlyingScraper.scrapeAddress(address)) {
      (a, b) =>
        a flatMap {
          case metadata if metadata.isComplete => Future.successful(metadata)
          case metadata => b.scrapeAddress(address).map(metadata.mergeWith)
        } recoverWith {
          case _ => b.scrapeAddress(address)
        }
    }
}
