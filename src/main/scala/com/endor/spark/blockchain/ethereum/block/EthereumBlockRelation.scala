package com.endor.spark.blockchain.ethereum.block

import java.io.{DataInputStream, EOFException}

import com.endor.spark.blockchain._
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.ethereum.core.Block

import scala.util.{Failure, Success, Try}

final case class EthereumBlockRelation(locations: String*)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {
  override def schema: StructType = SimpleEthereumBlock.encoder.schema

  private def parseRLPLengthWithIndicator(data: Array[Byte]): Int = {
    val detector = data(0)
    detector & 0xFF match {
      case unsignedDetector if unsignedDetector >= 0xc0 && unsignedDetector <= 0xf7 =>
        unsignedDetector
      case unsignedDetector =>
        val noOfBytesSize = unsignedDetector - 0xf7
        val indicator = Array[Byte](detector) ++ data.slice(1, noOfBytesSize + 1)
        indicator.length + indicator.tail.asInt
    }

  }

  private def readSingleBlock(is: DataInputStream): Option[Block] = {
    val listHeader = Array.fill[Byte](10)(0)
    Try {
      is.readFully(listHeader)
      val blockSize = parseRLPLengthWithIndicator(listHeader)
      val remainingData = Array.fill[Byte](blockSize.toInt - 10)(0)
      is.readFully(remainingData)
      new Block(listHeader ++ remainingData)
    } match {
      case Success(block) => Option(block)
      case Failure(_: EOFException) => None
      case Failure(err) => throw err
    }
  }

  def buildSimpleScan(): RDD[SimpleEthereumBlock] = {
    sqlContext.sparkContext
      .binaryFiles(locations.mkString(","))
      .flatMap {
        case (_: String, data: PortableDataStream) =>
          val is = data.open()
          Stream.continually(())
            .map(_ => readSingleBlock(is))
            .takeWhile(_.isDefined)
            .flatten
            .append {
              is.close()
              Seq.empty[Block]
            }
      }
      .map((block: Block) => SimpleEthereumBlock.fromEthereumjBlock(block))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  override def buildScan(): RDD[Row] = {
    val simpleScan = buildSimpleScan()
    simpleScan
      .map(Row.fromTuple)
  }
}
