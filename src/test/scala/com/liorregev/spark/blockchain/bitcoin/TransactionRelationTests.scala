package com.liorregev.spark.blockchain.bitcoin

import com.liorregev.spark.blockchain._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class TransactionRelationTests extends FunSuite with Matchers {
  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Correctly parse block version 1 (block 68956)") {
    val path = getClass.getResource("blocks/version1").toString
    val transactions = spark
      .read
      .bitcoin(path)
      .collect()

    println(s"Got ${transactions.length} transactions: ${transactions.map(_.transactionHash.hex).mkString(", ")}")
  }

  test("Correctly parse block version 2 (block 332208)") {
    val path = getClass.getResource("blocks/version2").toString
    val transactions = spark
      .read
      .bitcoin(path)
      .collect()

    println(s"Got ${transactions.length} transactions")
  }

  test("Correctly parse block version 3 (block 370505)") {
    val path = getClass.getResource("blocks/version3").toString
    val transactions = spark
      .read
      .bitcoin(path)
      .collect()

    println(s"Got ${transactions.length} transactions")
  }

  test("Correctly parse block version 4 (block 403617)") {
    val path = getClass.getResource("blocks/version4").toString
    val transactions = spark
      .read
      .bitcoin(path)
      .collect()

    println(s"Got ${transactions.length} transactions")
  }
}
