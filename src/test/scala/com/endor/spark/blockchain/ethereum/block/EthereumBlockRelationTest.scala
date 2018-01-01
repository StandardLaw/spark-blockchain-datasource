package com.endor.spark.blockchain.ethereum.block

import java.time.Instant

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
class EthereumBlockRelationTest extends FunSuite with EthereumTestUtils with Matchers {
  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  test("Correctly parse block 447533") {
    val expectedBlockHeader = EthereumBlockHeader(
      "043559b70c54f0eea6a90b384286d7ab312129603e750075d09fd35e66f8068a".bytes,
      "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347".bytes,
      "A027231f42C80Ca4125b5Cb962A21CD4f812e88f".bytes,
      "fc863fa242a2562d96c51b1dfe8ff1242486212ff5fcf71e4505482ebdc87a42".bytes,
      "bcacc758963319bda4240ee8b20553f2a3dc7819bf44caa38a1ecef926618df5".bytes,
      "1f81d0b487d586f2052387b3c9a482c809d2597995989f659c3b3afcafb2e770".bytes,
      Array.fill[Byte](256){0},
      6497405567250L.bytes,
      Instant.parse("2015-10-27T11:50:42Z").getEpochSecond,
      447533,
      3141592,
      1021000,
      "394d08eb778e71b7f2e149a02f17361518ae51221ba61f5e9701a3bd9ff7fee4".bytes,
      "6574682e70702e7561".bytes,
      "8b1a47758a1d7472".bytes
    )
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block447533.bin").toString
    val blocks = spark.read.ethereum(path).collect()
    blocks.length should be(1)

    val header = blocks.map(_.ethereumBlockHeader).head
    header should equal(expectedBlockHeader)
  }

  test("Correctly parse senders in block 447533") {
    val transactionSenders = Seq(
      "0047a8033cc6d6ca2ed5044674fd421f44884de8".bytes,
      "2910543af39aba0cd09dbb2d50200b3e800a63d2".bytes
    )
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block447533.bin").toString
    val blocks = spark.read.enrichedEthereum(path).collect()
    blocks.flatMap(_.ethereumTransactions).map(_.sendAddress) should contain theSameElementsInOrderAs transactionSenders
  }

  test("Correctly parse senders in block 84546") {
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block84546.bin").toString
    val blocks = spark.read.enrichedEthereum(path).collect()
    blocks.flatMap(_.ethereumTransactions).map(_.sendAddress.hex).head should be("c4c6baf00209a0f33331e4b7cb1c7b680a3d2f79")
  }

  test("Correctly parse senders in block 2800597 with post-parse enrichment") {
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block2800597.bin").toString
    val blocks = spark.read.ethereum(path).collect()
    val transaction = blocks.flatMap(_.ethereumTransactions).head.toEnriched
    transaction.sendAddress.hex should be("32be343b94f860124dc4fee278fdcbd38c102d88")
  }

  test("Parsing enriched blocks and enriching block after parsing yields the same results for block 447533") {
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block447533.bin").toString
    val blocks = spark.read.ethereum(path).collect()
    val enrichedBlocks = spark.read.enrichedEthereum(path).collect()

    blocks.map(_.toEnriched) should contain theSameElementsInOrderAs  enrichedBlocks
  }

  test("Parsing enriched blocks and enriching block after parsing yields the same results for block 2800597") {
    val path = getClass.getResource("/com/endor/spark/blockchain/ethereum/blocks/block2800597.bin").toString
    val blocks = spark.read.ethereum(path).collect()
    val enrichedBlocks = spark.read.enrichedEthereum(path).collect()

    blocks.map(_.toEnriched) should contain theSameElementsInOrderAs  enrichedBlocks
  }
}
