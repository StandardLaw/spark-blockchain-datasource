package com.endor.spark.blockchain.bitcoin

sealed trait Network {
  private[bitcoin] val id: String
}

object Network {
  case object Main extends Network { private[bitcoin] val id: String = "main" }
  case object Test extends Network { private[bitcoin] val id: String = "test" }
}
