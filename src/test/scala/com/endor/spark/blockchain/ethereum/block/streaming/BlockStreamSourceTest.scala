package com.endor.spark.blockchain.ethereum.block.streaming

import java.sql.Timestamp

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import com.endor.spark.blockchain.ethereum.token.TokenTransferEvent
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.FunSuite

import scala.concurrent.duration._


final case class ProcessedTransaction(timestamp: Timestamp, blockNumber: Long, nonce: String, value: Double,
                                      sendAddress: String, receiveAddress: String, gasPrice: Long, gasLimit: Long,
                                      data: Option[String], hash: String, contractAddress: Option[String])

object ProcessedTransaction {
  implicit val encoder: Encoder[ProcessedTransaction] = Encoders.product[ProcessedTransaction]
}

object ByteArrayUtil {
  def padByteArray(input: Array[Byte]): Array[Byte] =
    Array.fill[Byte](Math.max(java.lang.Long.BYTES - input.length, 1))(0) ++ input

  def convertByteArrayToDouble(input: Array[Byte], exponent: Int, decimalPrecision: Int): Double = {
    val cutOffBigInt = BigInt(Math.pow(10.0, exponent.toDouble).toLong)
    val inputBigInt = BigInt(padByteArray(input))
    (BigDecimal(inputBigInt / cutOffBigInt) / Math.pow(10.0, decimalPrecision.toDouble)).doubleValue()
  }
}


class BlockStreamSourceTest extends FunSuite {
  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  ignore("Simple test") {
    val blockDs = spark.readStream
      .option("databaseLocation", "/home/user/eth/db1")
      .format("com.endor.spark.blockchain.ethereum.block.streaming")
      .load()
      .as[SimpleEthereumBlock]
    val blocksQuery = blockDs
      .flatMap {
        block =>
          val blockTime = new Timestamp(block.ethereumBlockHeader.timestamp * 1000)
          val blockNumber = block.ethereumBlockHeader.number
          block.ethereumTransactions
            .map(_.toEnriched)
            .flatMap {
              transaction =>
                val original = ProcessedTransaction(blockTime, blockNumber, transaction.nonce.hex,
                  ByteArrayUtil.convertByteArrayToDouble(transaction.value, 15, 3), transaction.sendAddress.hex,
                  transaction.receiveAddress.hex, transaction.gasPrice, transaction.gasLimit,
                  transaction.data.map(_.hex), transaction.hash.hex,
                  transaction.contractAddress.map(_.hex))
                Seq(
                  original,
                  original.copy(sendAddress = original.receiveAddress, receiveAddress = original.sendAddress,
                    value = original.value * -1)
                )
            }
      }
      .writeStream
      .option("checkpointLocation", "/home/user/Desktop/BlockStreamParquet/checkpoints")
      .format("parquet")
      .queryName("syncEthereum")
      .trigger(Trigger.ProcessingTime(10 seconds))
      .start("/home/user/Desktop/BlockStreamParquet/data")

    val tokensDs = spark.readStream
      .option("databaseLocation", "/home/user/eth/db2")
      .format("com.endor.spark.blockchain.ethereum.token.streaming")
      .load()
      .as[TokenTransferEvent]

    val tokensQuery = tokensDs
      .writeStream
      .option("checkpointLocation", "/home/user/Desktop/TokenStreamParquet/checkpoints")
      .format("parquet")
      .queryName("syncEthereumTokens")
      .trigger(Trigger.ProcessingTime(10 seconds))
      .start("/home/user/Desktop/TokenStreamParquet/data")

    tokensQuery.awaitTermination(120000)
    tokensQuery.stop()
    blocksQuery.stop()
    tokensQuery.awaitTermination()
    blocksQuery.awaitTermination()
  }
}
