package com.endor.spark.blockchain.ethereum.token.metadata

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class CachedTokenMetadataScraper(underlyingScraper: TokenMetadataScraper) extends TokenMetadataScraper{

  private val cache: mutable.Map[String, Future[TokenMetadata]] = mutable.Map()

  override def scrapeAddress(address: String)(implicit ec: ExecutionContext): Future[TokenMetadata] =
    cache.getOrElseUpdate(address, underlyingScraper.scrapeAddress(address))
}
