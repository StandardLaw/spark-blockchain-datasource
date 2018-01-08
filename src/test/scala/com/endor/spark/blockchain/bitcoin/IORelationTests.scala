package com.endor.spark.blockchain.bitcoin

import com.endor.spark.blockchain.bitcoin.enriched._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{Json, OFormat}

import scala.collection.GenTraversable

class IORelationTests extends FunSuite with Matchers {
  import IORelationTests._

  private lazy val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  test("Correctly parse IO from block version 1 (block 68956)") {
    val path = getClass.getResource("blocks/version1").toString

    val expectedTransactions = Seq(
      "23b2743fe356f0cc125e19375eb37032ee4e7b8bb82c931238969e49366f1333",
      "8acc1082a9f4260065fb22d24700b2c0a5efb8fa18767aa8f40e91b3036b1515"
    )

    val actual: Seq[IO] = spark
      .read
      .bitcoin
      .transactions(path)
      .enriched
      .io
      .filter((io: IO) => expectedTransactions.contains(io.transactionHash))
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/68956.json"))
      .as[Seq[IO]]

    sameIO(actual, expected, allValuesMustExist = true, unknownAddressThreshold = 0.01)
  }

  test("Correctly parse IO from block version 2 (block 332208)") {
    val path = getClass.getResource("blocks/version2").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoin
      .transactions(path)
      .enriched
      .io
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/332208.json"))
      .as[Seq[IO]]

    sameIO(actual, expected, allValuesMustExist = false, unknownAddressThreshold = 0.01)
  }

  test("Correctly parse IO from block version 3 (block 370505)") {
    val path = getClass.getResource("blocks/version3").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoin
      .transactions(path)
      .enriched
      .io
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/370505.json"))
      .as[Seq[IO]]

    sameIO(actual, expected, allValuesMustExist = false, unknownAddressThreshold = 0.06) // Note: Threshold is higher here
  }

  test("Correctly parse IO from block version 4 (block 403617)") {
    val path = getClass.getResource("blocks/version4").toString
    val actual: Seq[IO] = spark
      .read
      .bitcoin
      .transactions(path)
      .enriched
      .io
      .collect()
      .toSeq

    val expected = Json.parse(getClass.getResourceAsStream("expected/403617.json"))
      .as[Seq[IO]]

    sameIO(actual, expected, allValuesMustExist = false, unknownAddressThreshold = 0.04) // Note: Threshold is higher here
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def sameIO(actualSeq: GenTraversable[IO], expectedSeq: Seq[IO], allValuesMustExist: Boolean, unknownAddressThreshold: Double): Unit = {
    actualSeq.size should equal(expectedSeq.size)

    val unknownAddressCount: Int = expectedSeq.sorted
      .zip(actualSeq.toList.sorted)
      .map { case (expected, actual) =>
        withClue(s"Expected: ${Json.toJsObject(expected).toString}, Actual: ${Json.toJsObject(actual).toString}") {
          actual.transactionHash should equal(expected.transactionHash)
          actual.isInput should equal(expected.isInput)
          actual.index should equal(expected.index)
          actual.timeMs should equal(expected.timeMs)

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
  implicit lazy val ioFormat: OFormat[IO] = Json.format[IO]

  implicit lazy val ioOrdering: Ordering[IO] = new Ordering[IO] {
    private val tupleOrdering = Ordering.Tuple3[String, Boolean, Long]

    override def compare(x: IO, y: IO): Int = {
      tupleOrdering.compare((x.transactionHash, x.isInput, x.index), (y.transactionHash, y.isInput, y.index))
    }
  }
}