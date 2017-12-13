package com.liorregev.spark.blockchain.ethereum.token

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.EthLog.LogObject
import org.web3j.protocol.http.HttpService

import scala.collection.JavaConverters._

final case class SourceHost(host: String, port: Int) {
  def getURL: String = s"http://$host:$port/"
}

object SourceHost {
  def fromString(value: String): SourceHost = {
    value.toString.split(':') match {
      case Array(host, port) =>
        SourceHost(host, port.toInt)
    }
  }
}

final case class TokenTransferEventsRelation(fromBlock: Long, toBlock: Long, numPartitions: Int,
                                             host: SourceHost, hosts: SourceHost*)
                                            (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  private val allHosts = hosts :+ host

  override def schema: StructType = TokenTransferEvent.encoder.schema

  def createPartitionDefs(): Seq[((Long, Long), String)] = {
    val numBlocks = toBlock - fromBlock + 1 // Add 1 because we are inclusive
    val partDefs = if(numBlocks < numPartitions) {
      (0 until numBlocks.toInt)
        .map(partId => (fromBlock + partId, fromBlock + partId))
    } else {
      val blocksPerPartition = Math.ceil(numBlocks.toDouble / numPartitions).toLong
      (0 until numPartitions)
        .map (partId => {
            val start = fromBlock + partId * blocksPerPartition
            val end = if (partId == numPartitions - 1) {
              toBlock
            } else {
              start + blocksPerPartition - 1
            }
            (start, end)
        })
    }
    partDefs
      .zip(Stream.continually(allHosts.toStream.map(_.getURL)).flatten)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  override def buildScan(): RDD[Row] = {
    val partitionDefs = createPartitionDefs()
    sqlContext.sparkContext
      .parallelize(partitionDefs)
      .flatMap {
        case ((from: Long, to: Long), currentHost: String) =>
          val web3j = Web3j.build(new HttpService(currentHost))
          val filter = new EthFilter(
            DefaultBlockParameter.valueOf(BigInt(from).bigInteger),
            DefaultBlockParameter.valueOf(BigInt(to).bigInteger),
            (Nil: List[String]).asJava
          ).addSingleTopic(TokenTransferEvent.topic)
          web3j.ethGetLogs(filter).send()
            .getLogs
            .asScala
            .collect {
              case log: LogObject => log
            }
            .flatMap(TokenTransferEvent.fromEthLog)
            .map(Row.fromTuple)
      }
  }
}
