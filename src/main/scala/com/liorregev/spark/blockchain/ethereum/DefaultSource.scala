package com.liorregev.spark.blockchain.ethereum

import com.liorregev.spark.blockchain.ethereum.datasource.EthereumBlockRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider  {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specified with files containing Bitcoin blockchain data."))
    val enrich = parameters.getOrElse("enrich", "false").toBoolean
    EthereumBlockRelation(path, enrich)(sqlContext)
  }
}
