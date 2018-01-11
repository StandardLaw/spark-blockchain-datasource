package com.endor.spark.blockchain.ethereum.token.metadata

import scala.concurrent.{ExecutionContext, Future}

trait TokenMetadataScraper {
  def scrapeAddress(address: String)(implicit ec: ExecutionContext): Future[TokenMetadata]
}