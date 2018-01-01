package com.endor.spark.blockchain.bitcoin.utils

import java.net.URL
import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.bitcoin.IORelationTests._
import com.endor.spark.blockchain.bitcoin.io.IO
import org.bitcoinj.core.NetworkParameters
import org.bitcoinj.params.MainNetParams
import play.api.libs.json._

import scala.sys.process._

object BlockDownloader extends App {
  // Change these when you need to download a new block
  val blockHeights: Seq[Int] = Seq(403617)
  val analyzeOutputProviders: Boolean = false // This takes a while. Use it if you need complete data about values for inputs.
  // -------------------------------------

  private val network: NetworkParameters = MainNetParams.get()

  private final case class Blocks(blocks: Seq[Block])
  private final case class Block(hash: String, ver: Int, time: Long, fee: Long, tx: Seq[Transaction], main_chain: Boolean)
  private final case class Transaction(hash: String, inputs: Seq[Input], out: Seq[Output])
  private final case class Input(prev_out: Option[Output])
  private final case class Output(addr: Option[String], value: Long, tx_index: Long)

  private implicit val blockFormat: OFormat[Blocks] = {
    implicit val output: OFormat[Output] = Json.format[Output]
    implicit val input: OFormat[Input] = Json.format[Input]
    implicit val transaction: OFormat[Transaction] = Json.format[Transaction]
    implicit val block: OFormat[Block] = Json.format[Block]

    Json.format[Blocks]
  }

  private def downloadBlock(blockHeight: Int): Unit = {
    val jsonBlock = {
      val json = new URL(s"https://blockchain.info/block-height/$blockHeight?format=json").cat !!<

      Json.parse(json).as[Blocks].blocks.filter(_.main_chain) match {
        case block :: Nil => block
        case _ => throw new Exception(s"More than one block on the main chain at https://blockchain.info/block-height/$blockHeight?format=json")
      }
    }

    {
      // Download raw block
      val hex = new URL(s"https://blockchain.info/block/${jsonBlock.hash}?format=hex").cat !!<
      val bytes = {
        val rawBytes = hex.trim.bytes

        ByteBuffer.allocate(rawBytes.length + Integer.BYTES + Integer.BYTES)
          .order(ByteOrder.BIG_ENDIAN)
          .putInt(network.getPacketMagic.toInt)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putInt(rawBytes.length)
          .put(rawBytes)
          .array()
      }

      val path = Paths.get(s"$blockHeight.blk")
      Files.write(path, bytes)

      println(s"Wrote ${bytes.length} bytes to ${path.toAbsolutePath}")
    }

    // Write "actual" data for tests
    val allIO = jsonBlock.tx.flatMap { tx =>
      val inputs = tx.inputs.zipWithIndex
        .collect { case (Input(Some(output)), index) =>
          IO(jsonBlock.hash.bytes, tx.hash.bytes, output.addr, Option(output.value), isInput = true, index.toLong, jsonBlock.time * 1000)
        }

      val outputs = tx.out.zipWithIndex.map { case (output, index) =>
        IO(jsonBlock.hash.bytes, tx.hash.bytes, output.addr, Option(output.value), isInput = false, index.toLong, jsonBlock.time * 1000)
      }

      inputs ++ outputs
    }

    if (analyzeOutputProviders) {
      val requiredBlocksText = {
        val requiredTransactions = jsonBlock.tx.flatMap(_.inputs.flatMap(_.prev_out.map(_.tx_index))).toSet

        val requiredBlocks = requiredTransactions
          .toList
          .zipWithIndex
          .flatMap { case (txIndex, index) =>
            println(s"Looking up ${index + 1}/${requiredTransactions.size}")

            val json = new URL(s"https://blockchain.info/tx-index/$txIndex?format=json").cat !!<

            Json.parse(json) \ "block_height" match {
              case JsDefined(value) =>
                Option(value.as[Long])

              case JsUndefined() =>
                println(s"Unable to find block height for transaction $txIndex. Please try manually doing so by going to https://blockchain.info/tx-index/$txIndex")
                None
            }
          }
          .distinct
          .sorted

        s"We will require the following blocks to have complete data about block #$blockHeight: ${requiredBlocks.mkString(", ")}"
      }

      val path = Paths.get(s"$blockHeight.txt")
      Files.write(path, requiredBlocksText.getBytes)

      println(s"Wrote note to ${path.toAbsolutePath}")
    }

    val jsonBody = Json.toJson {
      allIO
        .map {
          case io if io.isInput => io.copy(value = io.value.map(-_))
          case io => io
        }
        .map(comparable)
    }

    val path = Paths.get(s"$blockHeight.json")
    Files.write(path, Json.prettyPrint(jsonBody).getBytes)

    println(s"Wrote expected data to ${path.toAbsolutePath}")
  }

  blockHeights.foreach(downloadBlock)
}
