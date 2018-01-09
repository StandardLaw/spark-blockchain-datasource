package com.endor.spark.blockchain.ethereum.token

import org.scalatest.{FunSuite, Matchers}


class TokenNamesScraperTest extends FunSuite with Matchers {
  test("Getting EOS token data") {
    val address = "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0"
    val expectedMetadata = TokenMetadata(address, "EOS", "1000000000000000000000000000", 18)

    val scraper = new TokenMetadataScraper()
    val result = scraper.scrapeAddress(address)
    result shouldBe defined
    result.foreach(_ should equal (expectedMetadata))
  }
}
