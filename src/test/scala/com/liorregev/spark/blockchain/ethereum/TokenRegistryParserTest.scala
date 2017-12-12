package com.liorregev.spark.blockchain.ethereum

import com.liorregev.spark.blockchain._
import com.liorregev.spark.blockchain.ethereum.datasource.EthereumTestUtils
import com.liorregev.spark.blockchain.ethereum.model.EnrichedEthereumBlock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.scalatest.{FunSuite, Matchers}

class TokenRegistryParserTest extends FunSuite with EthereumTestUtils with Matchers {
  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Parse token register transaction 0xfc4cd1a0c8a80de6ad9e0f4ce42a9e3620c375e6f36d989435f411cf961ab58c") {
    val transactionHash = "fc4cd1a0c8a80de6ad9e0f4ce42a9e3620c375e6f36d989435f411cf961ab58c".bytes
    val path = getClass.getResource("/com/liorregev/spark/blockchain/ethereum/block4557262.bin").toString
    val txn = spark.read
      .enrichedEthereum(path)
      .flatMap((block: EnrichedEthereumBlock) => block.ethereumTransactions)
      .where(F.col("hash") equalTo transactionHash)
      .head

    val expectedResult = RegisterCall(
      "a823e6722006afe99e91c30ff5295052fe6b8e32".bytes,
      "NEU",
      BigDecimal(10).pow(18),
      "Neumark"
    )

    val result = TokenRegistryParser.parseRegisterCalls(txn.data.getOrElse(Array.empty))
    result shouldBe defined
    result.foreach(_ should equal(expectedResult))
  }
}
