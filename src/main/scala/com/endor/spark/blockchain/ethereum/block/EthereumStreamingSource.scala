package com.endor.spark.blockchain.ethereum.block

import com.endor.spark.streaming.BinaryFileStreamSource
import org.apache.spark.sql.{Dataset, SparkSession}

class EthereumStreamingSource(sparkSession: SparkSession,
                              path: String,
                              metadataPath: String,
                              options: Map[String, String])
  extends BinaryFileStreamSource[SimpleEthereumBlock](sparkSession, path, metadataPath, options) {
  override protected def createDataset(files: Seq[String]): Dataset[SimpleEthereumBlock] = {
    val relation = EthereumBlockRelation(files: _*)(sparkSession.sqlContext)
    import sparkSession.implicits._
    relation.buildSimpleScan().toDS()
  }
}