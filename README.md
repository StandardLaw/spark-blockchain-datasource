# spark-blockchain-datasource
[![Build Status](https://travis-ci.org/EndorCoin/spark-blockchain-datasource.svg?branch=master)](https://travis-ci.org/EndorCoin/spark-blockchain-datasource)

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to load various blockchains into Spark

Currently this datasource supports the following formats:
* Bitcoin as a dataset of Transaction case class using: com.endor.spark.blockchain.bitcoin.transactions
* Ethereum as a dataset of EthereumBlock case class using: com.endor.spark.blockchain.ethereum.block
* TokenMetadataScraper trait allows scraping token data as defined in the TokenMetadata case class from various sources: 
    * Actual contract calls
    * Ethplorer.io
    * Etherscan.io

# Develop
## Scala
### Ethereum
#### Blocks
This example loads Ethereum BlockChain data from the folder "/user/ethereum/input" using the format exported by `geth export`
```scala
import com.endor.spark.blockchain.ethereum._
import com.endor.spark.blockchain.ethereum.block.{EnrichedEthereumBlock, SimpleEthereumBlock}
import org.apache.spark.sql.{Dataset, SparkSession}

val spark: SparkSession = SparkSession.builder().appName("Ethereum Loader").getOrCreate()

val blocks: Dataset[SimpleEthereumBlock] = spark
   .read
   .ethereum("/user/ethereum/input")
   
val enrichedBlocks: Dataset[EnrichedEthereumBlock] = spark
   .read
   .enrichedEthereum("/user/ethereum/input")
```

#### Token Data
This example scrapes Ethereum Token data from three sources:
* Actual contract calls with JSON-RPC available at: http://myhost:myport/
* Ethplorer.io using api key "freekey"
* Etherscan.io
```scala
import com.endor.spark.blockchain.ethereum.token.metadata._

import akka.actor.ActorSystem
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

import scala.concurrent.{ExecutionContext, Future}

implicit val ec: ExecutionContext = ExecutionContext.global

val scraper: TokenMetadataScraper = {
  implicit val actorSystem: ActorSystem = ActorSystem()
  val web3j = Web3j.build(new HttpService(s"http://myhost:myport/"))
  new CachedTokenMetadataScraper(
    new CompositeTokenMetadataScraper(
      new Web3TokenMetadataScraper(web3j),
      new EthplorerTokenMetadataScraper("freekey"),
      new EtherscanTokenMetadataScraper()
    )
  )
}

val myTokenData: Future[TokenMetadata] = scraper.scrapeAddress("0xMyTokenAddress")
```
    
