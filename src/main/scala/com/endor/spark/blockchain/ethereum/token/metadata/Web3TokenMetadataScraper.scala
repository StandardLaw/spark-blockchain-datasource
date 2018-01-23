package com.endor.spark.blockchain.ethereum.token.metadata

import ch.qos.logback.classic.{Logger, LoggerContext}
import com.endor.spark.blockchain.ethereum.token.DetailedERC20
import org.web3j.protocol.Web3j
import org.web3j.tx.ClientTransactionManager
import rx.lang.scala.JavaConverters._

import scala.concurrent.{ExecutionContext, Future}


class Web3TokenMetadataScraper(web3j: Web3j)(implicit loggerFactory: LoggerContext) extends TokenMetadataScraper {
  private lazy val logger: Logger = loggerFactory.getLogger(this.getClass)
  private val bigInteger0 = BigInt(0).bigInteger
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private val transactionManager = new ClientTransactionManager(web3j, null)

  override def scrapeAddress(address: String)
                            (implicit ec: ExecutionContext): Future[TokenMetadata] = {
    logger.debug(s"Scraping web3 for $address")
    val contract = DetailedERC20.load(s"0x$address", web3j, transactionManager, bigInteger0, bigInteger0)
    val metadataObservable = for {
      decimals <- contract.decimals().observable().asScala
      symbol <- contract.symbol().observable().asScala
      totalSupply <- contract.totalSupply().observable().asScala
    } yield TokenMetadata(address, Option(symbol).filter(_.nonEmpty), Option(totalSupply.toString), Option(decimals.intValue()))
    metadataObservable.toBlocking.toFuture
  }
}
