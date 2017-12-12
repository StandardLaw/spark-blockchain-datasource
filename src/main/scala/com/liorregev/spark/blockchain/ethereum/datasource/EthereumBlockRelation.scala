package com.liorregev.spark.blockchain.ethereum.datasource

import java.io.InputStream

import com.liorregev.spark.blockchain.ethereum.model.{EnrichedEthereumBlock, SimpleEthereumBlock}
import com.liorregev.spark.blockchain._

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.ethereum.core.Block

final case class EthereumBlockRelation(location: String, enrich: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {
  override def schema: StructType = if(enrich) {
    EnrichedEthereumBlock.encoder.schema
  } else {
    SimpleEthereumBlock.encoder.schema
  }

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

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def loopedRead(inputStream: InputStream, amount: Int): Array[Byte] = {
    Stream.continually(()).scanLeft((Array.emptyByteArray, 0)) {
      case ((dataReadSoFar, _), _) =>
        val leftToRead = amount - dataReadSoFar.length
        val moreData = new Array[Byte](leftToRead)
        inputStream.read(moreData) match {
          case -1 =>
            (dataReadSoFar, -1)
          case amountRead =>
            (dataReadSoFar ++ moreData.slice(0, amountRead), amountRead)
        }
    }
      .dropWhile {
        case (_, lastRead) if lastRead == -1 => false
        case (dataReadSoFar, _) => dataReadSoFar.length < amount
      }
      .head._1
  }

  private def readSingleBlock(is: InputStream): Option[Block] = {
    val listHeader = loopedRead(is, 10)
    if (listHeader.length < 10) {
      None
    } else {
      val blockSize = parseRLPLengthWithIndicator(listHeader)
      val remainingData = loopedRead(is, blockSize.toInt - 10)
      if (remainingData.length < blockSize.toInt - 10) {
        None
      } else {
        Option(new Block(listHeader ++ remainingData))
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  override def buildScan(): RDD[Row] = {
    val toBlockFunction = if(enrich) {
      (block: Block) => Row.fromTuple(EnrichedEthereumBlock.fromEthereumjBlock(block))
    } else {
      (block: Block) => Row.fromTuple(SimpleEthereumBlock.fromEthereumjBlock(block))
    }
    sqlContext.sparkContext
      .binaryFiles(location)
      .flatMap {
        case (_: String, data: PortableDataStream) =>
          val is = data.open()
          Stream.continually(()).map(_ => readSingleBlock(is)).takeWhile(_.isDefined).flatten
      }
      .map(toBlockFunction)
  }
}
