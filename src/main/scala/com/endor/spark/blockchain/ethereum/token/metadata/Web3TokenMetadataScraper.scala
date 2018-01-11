package com.endor.spark.blockchain.ethereum.token.metadata

import com.endor.spark.blockchain.ethereum.token.DetailedERC20
import org.web3j.protocol.Web3j
import org.web3j.tx.ClientTransactionManager
import rx.lang.scala.JavaConverters._

import scala.concurrent.{ExecutionContext, Future}


class Web3TokenMetadataScraper(web3j: Web3j) extends TokenMetadataScraper {
  private val bigInteger0 = BigInt(0).bigInteger
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private val transactionManager = new ClientTransactionManager(web3j, null)

  override def scrapeAddress(address: String)
                            (implicit ec: ExecutionContext): Future[TokenMetadata] = {
    val contract = DetailedERC20.load(s"0x$address", web3j, transactionManager, bigInteger0, bigInteger0)
    val metadataObservable = for {
      decimals <- contract.decimals().observable().asScala
      symbol <- contract.symbol().observable().asScala
      totalSupply <- contract.totalSupply().observable().asScala
    } yield TokenMetadata(address, Option(symbol).filter(_.nonEmpty), Option(totalSupply.toString), Option(decimals.intValue()))
    metadataObservable.toBlocking.toFuture
  }
}
