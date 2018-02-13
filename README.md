# spark-blockchain-datasource

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to load various blockchains into Spark

Currently this datasource supports the following formats:
* Bitcoin as a dataset of Transaction case class using: com.endor.spark.blockchain.bitcoin.transactions
* Ethereum as a dataset of EthereumBlock case class using: com.endor.spark.blockchain.ethereum.block
* TokenMetadataScraper trait allows scraping token data as defined in the TokenMetadata case class from various sources: Actual contract calls, Ethplorer.io, Etherscan.io
