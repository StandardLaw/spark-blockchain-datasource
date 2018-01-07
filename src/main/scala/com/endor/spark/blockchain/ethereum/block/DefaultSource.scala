package com.endor.spark.blockchain.ethereum.block

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with StreamSourceProvider  {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specified with files containing Ethereum blockchain data."))
    EthereumBlockRelation(path)(sqlContext)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): (String, StructType) =
    ("ethereumBlocks", SimpleEthereumBlock.encoder.schema)

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source =
    new EthereumStreamingSource(sqlContext.sparkSession, parameters("path"), metadataPath, parameters)
}
