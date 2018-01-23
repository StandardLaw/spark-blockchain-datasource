package com.endor.spark.blockchain.ethereum.token

import akka.actor.ActorSystem
import com.endor.spark.blockchain.ethereum.token.metadata._
import org.scalatest.{FunSuite, Matchers}
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


class TokenMetadataScraperTest extends FunSuite with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem()

  ignore("Getting EOS token data from etherscan") {
    val address = "86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0"
    val expectedMetadata = TokenMetadata(address, Option("EOS"), Option("1000000000000000000000000000"), Option(18))

    val scraper = new EtherscanTokenMetadataScraper()
    val result = Await.result(scraper.scrapeAddress(address), 30 seconds)
    result should equal (expectedMetadata)
  }

  test("Getting REP token data from JSON-RPC") {
    val address = "E94327D07Fc17907b4DB788E5aDf2ed424adDff6"
    val expectedMetadata = TokenMetadata(address, Option("REP"), Option("11000000000000000000000000"), Option(18))

    val scraper = new Web3TokenMetadataScraper(Web3j.build(new HttpService(s"http://34.207.229.67:8545/")))
    val result = Await.result(scraper.scrapeAddress(address), 30 seconds)
    result should equal (expectedMetadata)
  }

  test("Getting OMGToken data from Ethplorer") {
    val address = "d26114cd6ee289accf82350c8d8487fedb8a0c07"
    val expectedMetadata = TokenMetadata(address, Option("OMG"), Option("140245398245132780789239631"), Option(18))
    val scraper = new EthplorerTokenMetadataScraper("freekey")
    val result = Await.result(scraper.scrapeAddress(address), 30 seconds)
    result should equal (expectedMetadata)
  }

  ignore("debug") {
    val scraper = {
      new CompositeTokenMetadataScraper(
        new Web3TokenMetadataScraper(Web3j.build(new HttpService(s"http://geth.endorians.com:8545/"))),
        new EthplorerTokenMetadataScraper("freekey"),
        new EtherscanTokenMetadataScraper()
      )
    }
    println(Await.result(scraper.scrapeAddress("11F8DD7699147566Cf193596083d45C8F592C4BA"), 1 minute))
  }
}
