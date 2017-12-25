package com.liorregev.spark.blockchain.bitcoin

import com.liorregev.spark.blockchain._
import com.liorregev.spark.blockchain.bitcoin.io.IO
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{Json, OFormat}

import scala.collection.GenTraversable

class IORelationTests extends FunSuite with Matchers {
  import IORelationTests._

  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Correctly parse IO from block version 1 (block 68956)") {
    val path = getClass.getResource("blocks/version1").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoinIO(path)
      .filter((io: IO) => io.blockHash.hex == "0000000000bef5d742ad3ea3fab0548bf33077a6a91426f37e6ea90bf40205dd")
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/68956.json"))
      .as[Seq[ComparableIO]]

    sameIO(actual.comparable, expected, allValuesMustExist = true, unknownAddressThreshold = 0.01)
  }

  test("Correctly parse IO from block version 2 (block 332208)") {
    val path = getClass.getResource("blocks/version2").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoinIO(path)
      .filter((io: IO) => io.blockHash.hex == "000000000000000007e5cc6f598ac0d10d5c0ae2abacef7373b83914c715636d")
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/332208.json"))
      .as[Seq[ComparableIO]]

    sameIO(actual.comparable, expected, allValuesMustExist = false, unknownAddressThreshold = 0.01)
  }

  test("Correctly parse IO from block version 3 (block 370505)") {
    val path = getClass.getResource("blocks/version3").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoinIO(path)
      .filter((io: IO) => io.blockHash.hex == "00000000000000000bf52d66c6e96aeb60cf1e3d7a07d7e55defaec9b6458845")
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/370505.json"))
      .as[Seq[ComparableIO]]

    sameIO(actual.comparable, expected, allValuesMustExist = false, unknownAddressThreshold = 0.06) // Note: Threshold is higher here
  }

  test("Correctly parse IO from block version 4 (block 403617)") {
    val path = getClass.getResource("blocks/version4").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoinIO(path)
      .filter((io: IO) => io.blockHash.hex == "00000000000000000245be9dd046492886b4527c7fa3494f26a9a9d876ecea22")
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/403617.json"))
      .as[Seq[ComparableIO]]

    sameIO(actual.comparable, expected, allValuesMustExist = false, unknownAddressThreshold = 0.04) // Note: Threshold is higher here
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def sameIO(actualSeq: GenTraversable[ComparableIO], expectedSeq: Seq[ComparableIO], allValuesMustExist: Boolean, unknownAddressThreshold: Double): Unit = {
    actualSeq.size should equal(expectedSeq.size)

    val unknownAddressCount: Int = expectedSeq.sorted
      .zip(actualSeq.toList.sorted)
      .map { case (expected, actual) =>
        withClue(s"Expected: ${Json.toJsObject(expected).toString}, Actual: ${Json.toJsObject(actual).toString}") {
          actual.blockHash should equal(expected.blockHash)
          actual.transactionHash should equal(expected.transactionHash)
          actual.isInput should equal(expected.isInput)
          actual.index should equal(expected.index)
          actual.blockTimeMs should equal(expected.blockTimeMs)

          (actual.value, expected.value) match {
            case (_, None) =>
              fail("Incomplete data for transaction! Find yourself a new block to test with.")
            case (None, _) if !allValuesMustExist =>
              // That's alright, we may not have this transaction in our history
            case (None, _) =>
              fail("All values must exist, but value does not exist")
            case (Some(actualValue), Some(expectedValue)) =>
              actualValue should equal(expectedValue)
          }

          (actual.address, expected.address) match {
            case (None, _) =>
              1 // Unknown address
            case (_, None) =>
              // We only care that this address was not found by us but was found by blockchain.info
              fail("Incomplete data for transaction! Find yourself a new block to test with.")
            case (Some(actualAddress), Some(expectedAddress)) =>
              actualAddress should equal(expectedAddress)
              0
          }
        }
      }
      .sum

    unknownAddressCount.should(be.<(Math.ceil(unknownAddressThreshold * expectedSeq.size).toInt))
  }
}

object IORelationTests {
  final case class ComparableIO(blockHash: String, transactionHash: String, address: Option[String], value: Option[Long], isInput: Boolean, index: Long, blockTimeMs: Long)

  def comparable(otherIO: IO): ComparableIO = {
    ComparableIO(otherIO.blockHash.hex, otherIO.transactionHash.hex, otherIO.address, otherIO.value, otherIO.isInput, otherIO.index, otherIO.blockTimeMs)
  }

  implicit class ComparableSeq[T <: GenTraversable[IO]](value: T) {
    def comparable: GenTraversable[ComparableIO] = value.map(IORelationTests.comparable)
  }

  implicit lazy val comparableIOFormat: OFormat[ComparableIO] = Json.format[ComparableIO]

  implicit lazy val comparableIOOrdering: Ordering[ComparableIO] = new Ordering[ComparableIO] {
    private val tupleOrdering = Ordering.Tuple3[String, Boolean, Long]

    override def compare(x: ComparableIO, y: ComparableIO): Int = {
      tupleOrdering.compare((x.transactionHash, x.isInput, x.index), (y.transactionHash, y.isInput, y.index))
    }
  }
}