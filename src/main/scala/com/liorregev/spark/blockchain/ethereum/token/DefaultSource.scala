package com.liorregev.spark.blockchain.ethereum.token

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider  {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val fromBlock = parameters.getOrElse("fromBlock",
      throw new IllegalArgumentException("Please specify fromBlock")).toLong
    val toBlock = parameters.getOrElse("toBlock",
      throw new IllegalArgumentException("Please specify toBlock")).toLong
    val numPartitions = parameters.getOrElse("numPartitions",
      sqlContext.getConf(SQLConf.SHUFFLE_PARTITIONS.key)).toInt
    val timeout = parameters.getOrElse("timeoutSeconds", "180").toLong
    val hosts = (parameters.get("hosts") orElse parameters.get("path"))
      .getOrElse(throw new IllegalArgumentException("Please specify hosts or path"))
      .split(',')
      .map(SourceHost.fromString)
    TokenTransferEventsRelation(fromBlock, toBlock, numPartitions, timeout, hosts.head, hosts.tail: _*)(sqlContext)
  }
}
