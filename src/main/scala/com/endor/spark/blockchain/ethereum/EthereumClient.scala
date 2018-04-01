package com.endor.spark.blockchain.ethereum

import org.ethereum.config.SystemProperties
import org.ethereum.facade.{Ethereum, EthereumFactory}

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object EthereumClient {
  private val ethereum: mutable.Map[String, Ethereum] = mutable.Map.empty

  def getClient(databaseLocation: String, syncEnabled: Boolean): Ethereum = {
    synchronized {
      ethereum.getOrElseUpdate(databaseLocation, {
        SystemProperties.getDefault.overrideParams()
        SystemProperties.getDefault.setSyncEnabled(syncEnabled)
        SystemProperties.getDefault.setDiscoveryEnabled(syncEnabled)
        SystemProperties.getDefault.setDataBaseDir(databaseLocation)
        EthereumFactory.createEthereum()
      })
    }
  }
}
