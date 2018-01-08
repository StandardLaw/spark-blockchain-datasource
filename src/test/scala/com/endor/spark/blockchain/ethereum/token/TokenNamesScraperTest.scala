package com.endor.spark.blockchain.ethereum.token

import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class TokenNamesScraperTest extends FunSuite with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("Getting EOS token data") {
    val address = "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0"

    val result = Await.result(TokenMetadata.getDataForAddress(address), 30 seconds)
    result shouldBe defined
    result.foreach(metadata => {
      metadata should equal(TokenMetadata(address, "EOS", "1000000000000000000000000000"))
    })
  }
}
