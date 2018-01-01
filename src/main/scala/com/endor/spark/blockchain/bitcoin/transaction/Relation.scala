package com.endor.spark.blockchain.bitcoin.transaction

import com.endor.spark.blockchain.bitcoin.BlockReader
import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.bitcoinj.core.{Block, Context, NetworkParameters}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

final case class Relation(location: String, network: String)
                         (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = Transaction.encoder.schema

  private lazy val logger: Logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  private lazy val networkParameters: NetworkParameters = {
    val params = NetworkParameters.fromPmtProtocolID(network)
    new Context(params) // Manually initialize a context for this thread if one does not exist
    params
  }

  private def blockToTransactions(block: Block): Seq[Transaction] = {
    block.getTransactions.asScala.map { bjt =>
      Transaction(
        block.getHash.getBytes,
        bjt.getHash.getBytes,
        bjt.getVersion,
        bjt.getInputs.size, bjt.getInputSum.value,
        bjt.getOutputs.size, bjt.getOutputSum.value,
        block.getTimeSeconds * 1000,
        bjt.getLockTime, bjt.getUpdateTime.getTime,
        Option(bjt.getFee).map(_.value)
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext
      .binaryFiles(location)
      .flatMap {
        case (_: String, data: PortableDataStream) =>
          val reader = new BlockReader(data.open(), networkParameters)

          Stream.continually(reader.nextBlock)
            .takeWhile {
              case Success(Left(BlockReader.EOF)) =>
                false
              case Failure(ex) =>
                logger.error("Error while deserializing block block", ex)
                false
              case _ =>
                true
            }
            .collect {
              case Success(Right(block)) => blockToTransactions(block)
            }
            .flatten
            .map((transaction: Transaction) => Row.fromTuple(transaction))
      }
  }
}

