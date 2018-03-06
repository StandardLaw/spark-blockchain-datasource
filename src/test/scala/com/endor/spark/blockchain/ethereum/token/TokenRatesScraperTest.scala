package com.endor.spark.blockchain.ethereum.token

import java.time.Instant

import org.scalatest.{FunSuite, Matchers}

class TokenRatesScraperTest extends FunSuite with Matchers {
  test("Getting EOS token rates") {
    val start = Instant.parse("2017-12-31T00:00:00Z")
    val end = Instant.parse("2018-01-01T00:00:00Z")
    val scraper = new TokenRatesScraper()
    val data = scraper.scrapeToken("eos", start, end)

    val expectedData = Seq(
      TokenRate("eos", java.sql.Date.valueOf("2017-12-31"), 8.51, 8.97, 8.44, 8.77, 4876510000L),
      TokenRate("eos", java.sql.Date.valueOf("2018-01-01"), 8.77, 9.11, 8.51, 8.84, 5041600000L)
    )

    data should contain theSameElementsAs expectedData
  }
}
